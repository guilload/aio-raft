import asyncio

from uuid import uuid4


class RaftPeer(object):
    def __init__(self, host, port, peer_id=None):
        self.host = host
        self.port = port
        self.peer_id = peer_id or uuid4()

        self.current = 0
        self.next = 0

        self._stream = None

    @asyncio.coroutine
    def stream(self):
        if self._stream is None:
            try:
                self._stream = yield from asyncio.open_connection(self.host, self.port)
            except ConnectionRefusedError:
                return None

        return self._stream
