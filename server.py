import asyncio
import json
import sys

from enum import Enum
from uuid import uuid4


class State(Enum):
    LEADER = 'Leader'
    FOLLOWER = 'Follower'
    CANDIDATE = 'Candidate'


class Server(object):
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
        self.votes = 0.

    def decode(self, data):
        return json.loads(data.decode())

    def encode(self, data):
        return json.dumps(data).encode()

    def call_election(self):
        self.state = State.CANDIDATE
        self.term += 1
        self.votes = 1.

        request = {'cmd': 'vote',
                   'type': 'response',
                   'uuid': self.uuid,
                   'log_index': 0,
                   'log_term': 0,
                   'term': self.term,
                   }
        self.broadcast(request)

    def append_entries(self):
        pass

    def broadcast(self, request):
        for peer in self.peers:
            asyncio.async(self.send(peer, request))

    @property
    def timedout(self):
        return self.last_heartbeat + self.heartbeat_interval < self.loop.time()

    @asyncio.coroutine
    def run(self):
        while True:
            if self.state == State.FOLLOWER and self.timedout:
                self.call_election()

            yield from asyncio.sleep(2)

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

        data = yield from reader.read(4096)
        response = self.decode(data)

        self.handle(response)

    def handle(self, response):
        print(response)
        # Update term and heartbeat

        if response['cmd'] == 'vote':
            self.handle_vote(response)

    def handle_vote(self, response):
        if self.term == response['term'] and response['granted']:
            self.votes += 1

            if self.votes / len(self.peers) > 0.5:
                print('I won the election!')
                self.state = State.LEADER
                self.append_entries()


def run(host, port):
    loop = asyncio.get_event_loop()
    peers = (('localhost', 9001), ('localhost', 9002))
    server = loop.run_until_complete(Server(loop, peers).run())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == '__main__':
    run('localhost', 8000)
