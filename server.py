import asyncio
import json
import random
import sys
import time

from enum import Enum
from uuid import uuid4


class State(Enum):
    LEADER = 'Leader'
    FOLLOWER = 'Follower'
    CANDIDATE = 'Candidate'


class Raft(object):

    def __init__(self, loop, peers, heartbeat_interval=1):
        self.loop = loop

        self.peers = peers
        self.streams = {}

        self.heartbeat_interval = heartbeat_interval
        self.last_heartbeat = loop.time()

        self.commit = 0
        self.log = []
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
    def leader(self):
        return self.state == State.LEADER

    @property
    def follower(self):
        return self.state == State.FOLLOWER

    @property
    def stale(self):
        return self.last_heartbeat + self.heartbeat_interval < self.loop.time()

    @staticmethod
    def decode(data):
        return json.loads(data.decode())

    @staticmethod
    def encode(data):
        return json.dumps(data).encode()

    @asyncio.coroutine
    def run(self):
        yield from asyncio.sleep(2)

        while True:
            start = self.loop.time()

            if self.leader:
                self.append_entries()

            if self.follower and self.stale:
                self.request_vote()

            end = self.loop.time()
            yield from asyncio.sleep()

    def broadcast(self, request):
        for peer in self.peers:
            asyncio.async(self.send(peer, request))

    def append_entries(self):
        request = {'rpc': 'append_entries_req',
                   'peer_id': self.uuid,
                   'term': self.term,
                   'commit': self.commit,
                   'entries': ['foo', 'bar'],
                   'log_index': 0,
                   'log_term': 0,
                   }

        self.broadcast(request)

    def request_vote(self):
        self.state = State.CANDIDATE
        self.term += 1
        self.voted = self.uuid
        self.votes = set([self.uuid])

        request = {'rpc': 'request_vote_req',
                   'peer_id': self.uuid,
                   'term': self.term,
                   'log_index': 0,
                   'log_term': 0,
                   }

        # yield from asyncio.sleep(random.randint())
        self.broadcast(request)

    @asyncio.coroutine
    def get_stream(self, peer):
        if peer in self.streams:
            stream = self.streams[peer]
        else:
            stream = yield from asyncio.open_connection(*peer, loop=self.loop)
            self.streams[peer] = stream

        return stream

    @asyncio.coroutine
    def send(self, peer, request):
        reader, writer = yield from self.get_stream(peer)
        writer.write(self.encode(request))

        data = yield from reader.read(4096)  # FIXME
        response = self.decode(data)

        self.handle(response)

    def handle(self, response):
        rpc = response['rpc']
        return self.handlers[rpc](response)

    def handle_append_entries_req(self, request):
        term = response['term']

        if self.candidate and self.term < term:
            self.state = State.FOLLOWER
            self.term = term
            self.voted = None
            self.votes = set()

        if term < self.term:
            success = False

        elif self.log and self.log[request['log_entry']]['term'] != request['log_term']:
            success = False

        else:
            success = True
            self.last_heartbeat = self.loop.time()

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

        elif False:  # FIXME invalid log case
            granted = False

        else:
            granted = True
            self.last_heartbeat = self.loop.time()

        return {'rpc': 'request_vote_resp',
                'peer_id': self.uuid,
                'term': self.term,
                'granted': granted,
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
        response = self.server.handle(request)
        self.transport.write(self.server.encode(response))


def run(port, ports):
    loop = asyncio.get_event_loop()
    peers = [('localhost', port) for port in ports]
    raft = Raft(loop, peers)
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
    time.sleep(2)
    run(port, ports)
