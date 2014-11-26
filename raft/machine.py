class Machine(dict):

    def apply(self, cmd):
        method, args = cmd.split(' ', 1)
        return getattr(self, method.upper())(args)

    def DEL(self, key):
        try:
            del self[key]
            return True
        except KeyError:
            return False

    def GET(self, key):
        try:
            return self[key]
        except KeyError:
            return False

    def SET(self, args):
        key, value = args.split(' ', 1)
        self[key] = value
        return True

