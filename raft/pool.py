from peer import Peer


class Pool(object):
    def __init__(self, server, peers):
        self._peers = {}

        for host, port in peers:
            peer = Peer(server, host, port)
            self._peers[peer.peer_id] = peer

    def __getitem__(self, key):
        return self._peers[key]

    def __iter__(self):
        for peer in self._peers.values():
            yield peer

    def __len__(self):
        return len(self._peers)

    def all(self):
        return self._peers.values()

    def ack(self, index):
        votes = sum(p.match >= index for p in self) + 1
        return self.majority(votes)  # +1 for the leader

    def majority(self, votes):
        return votes / (len(self) + 1) > 0.5
