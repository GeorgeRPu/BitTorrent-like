class Config:

    def __init__(self, filename):
        self.cfg = {}
        with open(filename, 'r') as f:
            for line in f.readlines():
                [key, value] = line.strip().split('=')
                self.cfg[key] = value

    def get_int(self, key):
        return int(self.cfg[key])

    def get_str(self, key):
        return str(self.cfg[key])
