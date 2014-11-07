import asyncio

from uuid import uuid4


class Peer(object):

    def __init__(self, server, host, port):
        self.host = host
        self.port = port
        self.peer_id = uuid4()

        self.current = 0
        self.next = 0

        self._loop = asyncio.get_event_loop()
        self._server = server
        self._transport = None

    @asyncio.coroutine
    def transport(self):
        if self._transport is None:
            factory = lambda: PeerProtocol(self, self._server)
            args = (factory, self.host, self.port)

            try:
                transport, _ = yield from self._loop.create_connection(*args)
            except ConnectionRefusedError:
                return None

            self._transport = transport

        return self._transport


class PeerProtocol(asyncio.Protocol):

    def __init__(self, peer, server):
        self.peer = peer
        self.server = server

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.peer._transport = None

    def data_received(self, data):
        request = self.server.decode(data)
        response = self.server.handle(request)

        if response:
            self.transport.write(self.server.encode(response))
