import json
import socket
import sys


def run(port, cmd):
    request = json.dumps({'rpc': 'client_req', 'cmd': cmd}).encode()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', int(port)))
    sock.sendall(request)
    sock.close()


if __name__ == '__main__':
    run(*sys.argv[1:])
