import asyncio
import json
import logging
import sys

from enum import Enum
from random import randint
from uuid import uuid4

from log import Log
from peer import Peer


class State(Enum):
    LEADER = 'Leader'
    FOLLOWER = 'Follower'
    CANDIDATE = 'Candidate'


class Server(object):

    def __init__(self, peers, host, port):
        self.host = host
        self.port = port

        self.logger = logging.getLogger(__name__)
        self.loop = asyncio.get_event_loop()

        self.peers = {}
        for h, p in peers:
            peer = Peer(self, h, p)
            self.peers[(h, p)] = peer

        # heartbeat constants and bookkeeping variables
        self.heartbeat_interval = 1000  # ms
        self.min_heartbeat_timeout = 2000  # ms
        self.max_heartbeat_timeout = 4000  # ms

        self.last_heartbeat = None
        self.last_interval = None
        self.heartbeat_timeout = None

        self.reset_heartbeat()
        self.reset_timeout()

        self.log = Log()
        self.state = State.FOLLOWER
        self.term = 0
        self.uuid = uuid4().hex
        self.voted = None
        self.votes = set()

        self.handlers = {'append_entries_req': self.handle_append_entries_req,
                         'append_entries_resp': self.handle_append_entries_resp,
                         'request_vote_req': self.handle_request_vote_req,
                         'request_vote_resp': self.handle_request_vote_resp,
                         'client_req': self.handle_client_req}

    def reset_heartbeat(self):
        self.last_heartbeat = self.loop.time()

    def reset_interval(self):
        self.last_interval = self.loop.time()

    def reset_timeout(self):
        self.heartbeat_timeout = randint(self.min_heartbeat_timeout,
                                         self.max_heartbeat_timeout) / 1000

    @property
    def stale(self):
        return self.last_heartbeat + self.heartbeat_timeout < self.loop.time()

    @staticmethod
    def decode(data):
        return json.loads(data.decode())

    @staticmethod
    def encode(data):
        return json.dumps(data).encode()

    def broadcast(self, request):
        for peer in self.peers.values():
            asyncio.async(self.send(peer, request))

    @asyncio.coroutine
    def run(self):
        self.reset_interval()

        while True:
            print(self.log._entries)
            print(self.log._applied)
            print(self.log.commit)
            self.logger.debug('state: {}, term: {}'.format(self.state,
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
        transport = yield from peer.transport()

        if transport is not None:
            transport.write(self.encode(request))

    @asyncio.coroutine
    def wait(self):
        """
        Wait for the next interval.
        """
        top = self.heartbeat_interval / 1000 - self.loop.time() + self.last_interval
        yield from asyncio.sleep(top)

        self.reset_interval()

    def to_leader(self):
        self.append_entries()
        self.state = State.LEADER
        self.voted = None
        self.votes = set()

        for peer in self.peers.values():
            peer.match = 0
            peer.next = self.log.index + 1

    def to_follower(self, term):
        self.state = State.FOLLOWER
        self.term = term
        self.voted = None
        self.votes = set()

    def append_entries(self, peer=None):
        """
        Append entries RPC.
        """
        peers = self.peers.values() if peer is None else [peer]

        for peer in peers:
            log_entries = self.log[peer.next:]
            log_index, log_term, _ = self.log[peer.next - 1]

            request = {'rpc': 'append_entries_req',
                       'peer_id': self.uuid,
                       'term': self.term,
                       'log_commit': self.log.commit,
                       'log_entries': log_entries,
                       'log_index': log_index,
                       'log_term': log_term,
                       }

            asyncio.async(self.send(peer, request))

        self.logger.debug('broadcasting append entries')

    def request_vote(self):
        """
        Request vote RPC.
        """
        self.reset_heartbeat()
        self.reset_timeout()

        self.state = State.CANDIDATE
        self.term += 1
        self.voted = self.uuid
        self.votes = set([self.uuid])

        request = {'rpc': 'request_vote_req',
                   'peer_id': self.uuid,
                   'term': self.term,
                   'log_index': self.log.index,
                   'log_term': self.log.term,
                   }

        self.broadcast(request)
        self.logger.debug('broadcasting request vote')

    def handle(self, request):
        """
        Dispatch requests to the appropriate handlers.
        """
        if self.term < request.get('term', self.term):
            self.to_follower(request.get('term', self.term))

        return self.handlers[request['rpc']](request)

    def handle_append_entries_req(self, request):
        self.logger.debug('append entries request received')

        if request.get('term', self.term) < self.term:
            return

        self.reset_heartbeat()

        log_index = request['log_index']
        log_term = request['log_term']

        if not self.log.match(log_index, log_term):
            return {'rpc': 'append_entries_resp',
                    'peer_id': self.uuid,
                    'term': self.term,
                    'success': False
                    }

        entries = request['log_entries']
        self.log.append(log_index, entries)

        commit = request['log_commit']
        if self.log.commit < commit:
            self.log.commit = min(self.log.index, commit)

        if not entries:
            return

        return {'rpc': 'append_entries_resp',
                'peer_id': (self.host, self.port),
                'term': self.term,
                'log_index': self.log.index,
                'success': True,
                }

    def handle_append_entries_resp(self, response):
        if response['success']:
            self.logger.debug('append entries succeeded')

            log_index = response['log_index']
            peer_id = tuple(response['peer_id'])
            self.peers[peer_id].match = log_index
            self.peers[peer_id].next = log_index + 1

            print(self.log.commit < log_index)
            print(sum(1 if p.match <= log_index else 0 for p in self.peers.values()) + 1)
            print(self.log[log_index][1] == self.term)

            if self.log.commit < log_index and (sum(1 if p.match <= log_index else 0 for p in self.peers.values()) + 1)/ len(self.peers) > 0.5 and self.log[log_index][1] == self.term:
                self.log.commit = log_index
        else:
            peer = self.peers[response['peer_id']]
            peer.next -= 1
            self.append_entries(peer)

            # self.logger.debug('append entries failed')

    def handle_request_vote_req(self, request):
        self.logger.debug('request vote request received')

        if request.get('term', self.term) < self.term:
            return

        log_index = request['log_index']
        log_term = request['log_term']
        peer_id = request['peer_id']

        if self.voted in (None, peer_id) and self.log.match(log_index, log_term):
            granted = True
            self.reset_heartbeat()
        else:
            granted = False

        return {'rpc': 'request_vote_resp',
                'peer_id': self.uuid,
                'term': self.term,
                'granted': granted,
                }

    def handle_request_vote_resp(self, response):
        if self.term == response['term'] and response['granted']:
            self.votes.add(response['peer_id'])

            if len(self.votes) / len(self.peers) > 0.5:
                self.to_leader()

    def handle_client_req(self, request):
        print(request)
        self.log.add(self.term, request['cmd'])
        self.append_entries()


class ServerProtocol(asyncio.Protocol):

    def __init__(self, server):
        self.server = server

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        pass

    def data_received(self, data):
        request = self.server.decode(data)
        response = self.server.handle(request)

        if response:
            self.transport.write(self.server.encode(response))


def run(port, ports):
    loop = asyncio.get_event_loop()
    peers = [('localhost', port) for port in ports]
    raft = Server(peers, 'localhost', port)
    server = loop.create_server(lambda: ServerProtocol(raft), 'localhost', port)
    coroutines = asyncio.gather(raft.run(), server)

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
