import asyncio
import json
import logging
import sys

from enum import Enum
from random import randint
from uuid import uuid4

from log import RaftLog
from peer import RaftPeer


LOGGER = logging.getLogger(__name__)


class State(Enum):
    LEADER = 'Leader'
    FOLLOWER = 'Follower'
    CANDIDATE = 'Candidate'


class RaftServer(object):

    def __init__(self, peers):
        self.loop = asyncio.get_event_loop()

        self.peers = {}

        for host, port in peers:
            peer = RaftPeer(host, port)
            self.peers[peer.peer_id] = peer

        # heartbeat constants and bookkeeping variables
        self.interval = 1
        self.timeout = randint(2, 4)
        self.last_heartbeat = self.loop.time()
        self.last_interval = self.loop.time() - self.interval

        self.log = RaftLog()
        self.state = State.FOLLOWER
        self.term = 0
        self.uuid = uuid4().hex
        self.voted = None
        self.votes = set()

        self.handlers = {'append_entries_req': self.handle_append_entries_req,
                         'append_entries_resp': self.handle_append_entries_resp,
                         'request_vote_req': self.handle_request_vote_req,
                         'request_vote_resp': self.handle_request_vote_resp}

    @property
    def stale(self):
        return self.last_heartbeat + self.timeout < self.loop.time()

    @staticmethod
    def decode(data):
        return json.loads(data.decode())

    @staticmethod
    def encode(data):
        return json.dumps(data).encode()

    @asyncio.coroutine
    def run(self):
        while True:
            print(self.state, self.term, self.stale)

            if self.state == State.LEADER:
                self.append_entries()

            if self.state in (State.CANDIDATE, State.FOLLOWER) and self.stale:
                self.request_vote()

            yield from self.sleep()

    @asyncio.coroutine
    def sleep(self):
        """
        Sleeps until the next interval.
        """
        delay = self.interval - self.loop.time() + self.last_interval
        yield from asyncio.sleep(delay)

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
                   'commit': self.log.commit,
                   'entries': [],
                   'log_index': self.log.index,
                   'log_term': self.log.term,
                   }

        self.broadcast(request)

    def request_vote(self):
        self.last_heartbeat = self.loop.time()
        self.state = State.CANDIDATE
        self.term += 1
        self.timeout = randint(150, 300) / 1000
        self.voted = self.uuid
        self.votes = set([self.uuid])

        request = {'rpc': 'request_vote_req',
                   'peer_id': self.uuid,
                   'term': self.term,
                   'log_index': self.log.index,
                   'log_term': self.log.term,
                   }

        self.broadcast(request)

    @asyncio.coroutine
    def send(self, peer, request):
        stream = yield from peer.stream()

        if stream is not None:
            reader, writer = stream
            writer.write(self.encode(request))

            data = yield from reader.read(4096)  # FIXME see http://bugs.python.org/issue20154
            response = self.decode(data)

            self.handle(response)

    def handle(self, response):
        LOGGER.debug('Response received: {}'.format(response))

        if self.term < response['term']:
            self.to_follower(response['term'])

        return self.handlers[response['rpc']](response)

    def to_follower(self, term):
        self.state = State.FOLLOWER
        self.term = term
        self.voted = None
        self.votes = set()

    def handle_append_entries_req(self, request):
        if request['term'] < self.term:
            success = False

        elif not self.log.match(request['log_index'], request['log_term']):
            success = False

        else:
            success = True
            self.log.append(request['entries'])
            self.last_heartbeat = self.loop.time()
            # if request['commit'] > self.log.commit:
            #     self.log.commit = min(request['commit'], request['entries'][-1)

        return {'rpc': 'append_entries_resp',
                'peer_id': self.uuid,
                'term': self.term,
                'success': success,
                }

    def handle_append_entries_resp(self, response):
        if response['success']:
            self.term = response['term']

    def handle_request_vote_req(self, request):
        if request['term'] < self.term:
            granted = False

        elif self.voted in (None, request['peer_id']) and self.log.match(request['log_index'],
                                                                         request['log_term']):
            granted = True
            self.last_heartbeat = self.loop.time()

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


class RaftProtocol(asyncio.Protocol):

    def __init__(self, server):
        self.server = server

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        request = self.server.decode(data)

        term = request['term']
        if self.server.term < term:
            self.server.to_follower(term)

        response = self.server.handle(request)
        self.transport.write(self.server.encode(response))


def run(port, ports):
    loop = asyncio.get_event_loop()
    peers = [('localhost', port) for port in ports]
    raft = RaftServer(peers)
    server = loop.create_server(lambda: RaftProtocol(raft), 'localhost', port)
    coroutines = asyncio.gather(raft.run(), server)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # server.close()
    # loop.run_until_complete(server.wait_closed())
    # loop.close()


if __name__ == '__main__':
    port, *ports = map(int, sys.argv[1:])
    run(port, ports)
