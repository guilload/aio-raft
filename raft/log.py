class Log(object):

    def __init__(self, machine):
        self.applied = -1
        self.commit = -1
        self.machine = machine

        self._entries = []  # entries are tuples (index, term, cmd)

    def __getitem__(self, key):
        if key == -1:  # starting state
            return (-1, -1, None)
        else:
            return self._entries[key]

    def __len__(self):
        return len(self._entries)

    def apply(self, commit):
        results = []

        while self.applied < commit:
            self.applied += 1
            index, term, cmd = self[self.applied]
            result = self.machine.apply(cmd)

            results.append((term, index, result))

        return results

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

    def match(self, index, term):
        try:
            _, trm, _ = self[index]
        except IndexError:
            return False

        return trm == term
