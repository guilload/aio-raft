import asyncio
import json
import logging
import sys

from enum import Enum
from random import randint
from uuid import uuid4

from log import Log
from machine import Machine
from pool import Pool
from protocols import ClientProtocol, ServerProtocol


class State(Enum):
    LEADER = 'Leader'
    FOLLOWER = 'Follower'
    CANDIDATE = 'Candidate'


class Server(object):

    def __init__(self, peers, host, port):
        self.host = host
        self.port = port
        self.peer_id = '{}:{}'.format(host, port)

        self._logger = logging.getLogger(__name__)
        self._loop = asyncio.get_event_loop()
        self._pool = Pool(self, peers)

        # heartbeat constants and bookkeeping variables
        self._heartbeat_interval = 1000  # ms
        self._last_interval = None

        self._min_heartbeat_timeout = 2000  # ms
        self._max_heartbeat_timeout = 4000  # ms
        self._heartbeat_timeout = None
        self._last_heartbeat = None

        self.reset_heartbeat()
        self.reset_timeout()

        self._log = Log(Machine())
        self.state = State.FOLLOWER
        self.term = 0
        self.voted = None
        self.votes = set()

        self._pending_clients = {}

        self.handlers = {'append_entries_req': self.handle_append_entries_req,
                         'append_entries_resp': self.handle_append_entries_resp,
                         'request_vote_req': self.handle_request_vote_req,
                         'request_vote_resp': self.handle_request_vote_resp}

    def reset_heartbeat(self):
        self._last_heartbeat = self._loop.time()

    def reset_interval(self):
        self._last_interval = self._loop.time()

    def reset_timeout(self):
        self._heartbeat_timeout = randint(self._min_heartbeat_timeout,
                                          self._max_heartbeat_timeout) / 1000

    @property
    def stale(self):
        return self._last_heartbeat + self._heartbeat_timeout < self._loop.time()

    @staticmethod
    def decode(data):
        return json.loads(data.decode())

    @staticmethod
    def encode(data):
        return json.dumps(data).encode()

    def broadcast(self, request):
        for peer in self._pool:
            self.send_async(peer, request)

    @asyncio.coroutine
    def run(self):
        self.reset_interval()

        while True:
            self._logger.debug('state: {}, term: {}'.format(self.state,
                                                            self.term))

            if self.state == State.LEADER:
                self.append_entries()

            if self.state in (State.CANDIDATE, State.FOLLOWER) and self.stale:
                self.request_vote()

            yield from self.wait()

    @asyncio.coroutine
    def send(self, peer, request):
        """
        Send a request to a peer (if available).
        """
        transport = yield from peer.get_transport()

        if transport:
            transport.write(self.encode(request))

    def send_async(self, peer, request):
        """
        Schedule the execution
        """
        asyncio.async(self.send(peer, request))

    @asyncio.coroutine
    def wait(self):
        """
        Wait for the next interval.
        """
        tic = self._heartbeat_interval / 1000 - self._loop.time() + self._last_interval
        yield from asyncio.sleep(tic)

        self.reset_interval()

    def to_leader(self):
        self.append_entries()
        self.state = State.LEADER
        self.voted = None
        self.votes = set()

        for peer in self._pool:
            peer.match = -1
            peer.next = self._log.index + 1

    def to_follower(self, term):
        self.state = State.FOLLOWER
        self.term = term
        self.voted = None
        self.votes = set()

    def append_entries(self, peer=None):
        """
        Append entries RPC.
        """
        peers = self._pool.all() if peer is None else [peer]

        for peer in peers:
            log_entries = self._log[peer.next:]
            log_index, log_term, _ = self._log[peer.next - 1]

            request = {'rpc': 'append_entries_req',
                       'peer_id': self.peer_id,
                       'term': self.term,
                       'log_commit': self._log.commit,
                       'log_entries': log_entries,
                       'log_index': log_index,
                       'log_term': log_term,
                       }

            self.send_async(peer, request)

        self._logger.debug('broadcasting append entries')

    def request_vote(self):
        """
        Request vote RPC.
        """
        self.reset_heartbeat()
        self.reset_timeout()

        self.state = State.CANDIDATE
        self.term += 1
        self.voted = self.peer_id
        self.votes = set([self.peer_id])

        request = {'rpc': 'request_vote_req',
                   'peer_id': self.peer_id,
                   'term': self.term,
                   'log_index': self._log.index,
                   'log_term': self._log.term,
                   }

        self.broadcast(request)
        self._logger.debug('broadcasting request vote')

    def handle_peer(self, request):
        """
        Dispatch requests to the appropriate handlers.
        """
        if self.term < request['term']:
            self.to_follower(request['term'])

        return self.handlers[request['rpc']](request)

    def handle_append_entries_req(self, request):
        self._logger.debug('append entries request received')

        if request['term'] < self.term:
            return

        self.reset_heartbeat()

        log_index = request['log_index']
        log_term = request['log_term']

        if not self._log.match(log_index, log_term):
            return {'rpc': 'append_entries_resp',
                    'peer_id': self.peer_id,
                    'term': self.term,
                    'log_index': self._log.index,
                    'success': False
                    }

        log_entries = request['log_entries']
        self._log.append(log_index, log_entries)

        log_commit = request['log_commit']
        if self._log.commit < log_commit:
            index = min(self._log.index, log_commit)
            self._log.commit = index
            self._log.apply(index)

        if not log_entries:  # no need to answer, the peer might have committed
            return  # new entries but has certainly not replicated new ones

        return {'rpc': 'append_entries_resp',
                'peer_id': self.peer_id,
                'term': self.term,
                'log_index': self._log.index,
                'log_term': self._log.term,
                'success': True,
                }

    def handle_append_entries_resp(self, response):
        if response['success']:
            self._logger.debug('append entries succeeded')

            log_index = response['log_index']
            log_term = response['log_term']
            peer_id = response['peer_id']

            self._pool[peer_id].match = log_index
            self._pool[peer_id].next = log_index + 1

            if (self._log.commit < log_index and
                self._pool.ack(log_index) and log_term == self.term):
                self._log.commit = log_index
                results = self._log.apply(log_index)
                self.return_results(results)

        else:
            peer = self._pool[response['peer_id']]
            peer.next -= 1
            self.append_entries(peer)

            # self._logger.debug('append entries failed')

    def handle_request_vote_req(self, request):
        self._logger.debug('request vote request received')

        if request['term'] < self.term:
            return

        log_index = request['log_index']
        log_term = request['log_term']
        peer_id = request['peer_id']

        if self.voted in (None, peer_id) and self._log.match(log_index, log_term):
            granted = True
            self.reset_heartbeat()
        else:
            granted = False

        return {'rpc': 'request_vote_resp',
                'peer_id': self.peer_id,
                'term': self.term,
                'granted': granted,
                }

    def handle_request_vote_resp(self, response):
        if self.term == response['term'] and response['granted']:
            self.votes.add(response['peer_id'])

            if self._pool.majority(len(self.votes)):
                self.to_leader()

    def handle_client(self, cmd, transport):
        self._log.add(self.term, cmd)
        self._pending_clients[(self.term, self._log.index)] = transport
        self.append_entries()

    def return_results(self, results):
        for result in results:
            term, index, result = result
            transport = self._pending_clients.pop((term, index))
            transport.write(self.encode(result))
            transport.close()


def run(port, ports):
    loop = asyncio.get_event_loop()
    peers = [('localhost', port) for port in ports]
    raft = Server(peers, 'localhost', port)
    client = loop.create_server(lambda: ClientProtocol(raft), 'localhost', port + 10)
    server = loop.create_server(lambda: ServerProtocol(raft), 'localhost', port)
    coroutines = asyncio.gather(client, server, raft.run())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # server.close()
    # loop.run_until_complete(server.wait_closed())
    # loop.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    port, *ports = map(int, sys.argv[1:])
    run(port, ports)
