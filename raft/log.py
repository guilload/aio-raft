class Log(object):

    def __init__(self):
        self.applied = 0
        self.commit = 0
        self.index = 1
        self.term = 0

        self.entries = []

    def append(*args):
        pass

    def match(self, index, term):
        return self.index == index and self.term == term
