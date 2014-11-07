import asyncio

from uuid import uuid4

from protocols import PeerProtocol


class Peer(object):

    def __init__(self, server, host, port):
        self.host = host
        self.port = port
        self.peer_id = '{}:{}'.format(host, port)

        self.match = -1
        self.next = 0
        self.transport = None
        self.up = False

        self._loop = asyncio.get_event_loop()
        self._create_conn = self._loop.create_connection  # pep8... see below
        self._server = server

    @asyncio.coroutine
    def get_transport(self):
        if self.transport is None:
            factory = lambda: PeerProtocol(self, self._server)
            args = factory, self.host, self.port

            try:
                self.transport, _ = yield from self._create_conn(*args)
            except ConnectionRefusedError:
                return

            self.up = True

        return self.transport
