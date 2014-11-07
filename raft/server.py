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

    def __init__(self, peers):
        self.logger = logging.getLogger(__name__)
        self.loop = asyncio.get_event_loop()

        self.peers = {}
        for host, port in peers:
            peer = Peer(self, host, port)
            self.peers[peer.peer_id] = peer

        # heartbeat constants and bookkeeping variables
        self.heartbeat_interval = 0.1
        self.min_heartbeat_timeout = 200
        self.max_heartbeat_timeout = 300

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
                         'request_vote_resp': self.handle_request_vote_resp}

    def reset_heartbeat(self):
        self.last_heartbeat = self.loop.time()

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

    @asyncio.coroutine
    def run(self):
        self.last_interval = self.loop.time()

        while True:
            self.logger.debug('state: {}, term: {}'.format(self.state,
                                                           self.term))

            if self.state == State.LEADER:
                self.append_entries()

            if self.state in (State.CANDIDATE, State.FOLLOWER) and self.stale:
                self.request_vote()

            yield from self.wait()

    @asyncio.coroutine
    def wait(self):
        """
        Waits for the next interval.
        """
        top = self.heartbeat_interval - self.loop.time() + self.last_interval
        yield from asyncio.sleep(top)

        self.last_interval = self.loop.time()

    def broadcast(self, request, delay=0):
        for peer in self.peers.values():
            asyncio.async(self.send(peer, request))

    def append_entries(self):
        """
        Append entries RPC.
        """
        request = {'rpc': 'append_entries_req',
                   'peer_id': self.uuid,
                   'term': self.term,
                   'log_commit': self.log.commit,
                   'log_entries': [],
                   'log_index': self.log.index,
                   'log_term': self.log.term,
                   }

        self.broadcast(request)
        self.logger.debug('broadcasting append entries')

    def request_vote(self):
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

    @asyncio.coroutine
    def send(self, peer, request):
        transport = yield from peer.transport()

        if transport is not None:
            transport.write(self.encode(request))

    def handle(self, request):
        if self.term < request['term']:
            self.to_follower(request['term'])

        return self.handlers[request['rpc']](request)

    def to_follower(self, term):
        self.state = State.FOLLOWER
        self.term = term
        self.voted = None
        self.votes = set()

    def handle_append_entries_req(self, request):
        self.logger.debug('append entries request received')

        if request['term'] < self.term:
            return

        self.reset_heartbeat()

        # log_index = request['log_index']
        # log_term = request['log_term']

        # if not self.log.match(log_index, log_term):
        #     return {'rpc': 'append_entries_resp',
        #             'peer_id': self.uuid,
        #             'term': self.term,
        #             'log_index': self.log.index,
        #             'log_term': self.log.term,
        #             'success': False
        #             }

        # commit = request['commit']

        # if self.log.commit < commit:
        #     self.log.commit = commit
        #     # FIXME

        entries = request['log_entries']

        if not entries:
            return  # heartbeat

        self.log.append(entries)
        return {'rpc': 'append_entries_resp',
                'peer_id': self.uuid,
                'log_term': self.term,
                'success': success,
                'log_index': self.log.index
                }

    def handle_append_entries_resp(self, response):
        if response['success']:
            self.term = response['term']
            self.logger.debug('append entries succeeded')
        else:
            self.logger.debug('append entries failed')

    def handle_request_vote_req(self, request):
        self.logger.debug('request vote request received')

        if request['term'] < self.term:
            return

        log_index = request['log_index']
        log_term = request['log_term']
        peer_id = request['peer_id']

        if self.voted in (None, peer_id) and self.log.match(log_index, log_term):
            granted = True
            self.last_heartbeat = self.loop.time()  # XXX: ???
        else:
            granted = False

        return {'rpc': 'request_vote_resp',
                'peer_id': self.uuid,
                'term': self.term,
                'granted': granted
                }

    def handle_request_vote_resp(self, response):
        if self.term == response['term'] and response['granted']:
            self.votes.add(response['peer_id'])

            if len(self.votes) / len(self.peers) > 0.5:
                self.append_entries()
                self.state = State.LEADER
                self.voted = None
                self.votes = set()


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
    raft = Server(peers)
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
