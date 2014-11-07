class Log(object):

    def __init__(self):
        self._applied = -1
        self._commit = -1
        self._entries = []  # entries are tuples (index, term, cmd)

    def __getitem__(self, key):
        if key == -1:  # starting state
            return (-1, -1, None)
        else:
            return self._entries[key]

    def __len__(self):
        return len(self._entries)

    @property
    def commit(self):
        return self._commit

    @commit.setter
    def commit(self, value):
        assert self._commit < value

        self._commit = value

        while self._applied < self._commit:
            self._applied += 1
            self.apply(self._applied)

    @property
    def index(self):
        return len(self) - 1  # -1 when empty

    @property
    def term(self):
        _, term, _ = self[self.index]
        return term

    def add(self, term, cmd):
        self._entries.append((self.index + 1, term, cmd))

    def append(self, start, entries):
        assert start <= len(self)

        self._entries = self._entries[:start + 1]
        self._entries.extend(entries)

    def apply(self, index):
        pass

    def match(self, index, term):
        try:
            _, trm, _ = self[index]
        except IndexError:
            return False

        return trm == term
