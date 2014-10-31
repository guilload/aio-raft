import asyncio
import json
import sys


class DummyPeer(asyncio.Protocol):

    def decode(self, data):
        return json.loads(data.decode())

    def encode(self, data):
        return json.dumps(data).encode()

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        self.transport = transport

    def data_received(self, data):
        request = self.decode(data)
        print('Request received: {!r}'.format(request))

        response = {'cmd': 'vote', 'term': request['term'], 'granted': True}
        print('Send: {!r}'.format(response))
        self.transport.write(self.encode(response))

        # print('Close the client socket')
        # self.transport.close()


def run(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(DummyPeer, host, port)
    server = loop.run_until_complete(coro)

    print('Serving on {}'.format(server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == '__main__':
    run(*sys.argv[1:])
