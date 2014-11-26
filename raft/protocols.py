import asyncio


class ClientProtocol(asyncio.Protocol):  # TODO factorize duplicate protocols

    def __init__(self, server):
        self.server = server
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        request = self.server.decode(data)
        self.server.handle_client(request, self.transport)


class PeerProtocol(asyncio.Protocol):

    def __init__(self, peer, server):
        self.peer = peer
        self.server = server
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, _):
        self.peer.transport = None
        self.peer.up = False

    def data_received(self, data):
        request = self.server.decode(data)
        response = self.server.handle_peer(request)

        if response:
            self.transport.write(self.server.encode(response))


class ServerProtocol(asyncio.Protocol):

    def __init__(self, server):
        self.server = server
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        request = self.server.decode(data)
        response = self.server.handle_peer(request)

        if response:
            self.transport.write(self.server.encode(response))
