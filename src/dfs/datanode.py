import Pyro4
from utils.conf import load_config

class DataNode:
    """ A data node in distributed file system """

    def __init__(self, conf):
        self.conf = load_config(conf)
        self.files = {}

    def run(self):
        Pyro4.Daemon.serveSimple(
            {
                self: self.conf['name']
            },
            port=int(self.conf['port']),
            ns=True)

    def create_file(self, filename):
        pass

    def delete_file(self, filename):
        pass

    def read_file(self, filename, offset, nbytes):
        f = open(filename, 'r')
        f.seek(offset)
        return f.read(nbytes)

    def readline_file(self, filename, offset):
        f = open(filename, 'r')
        f.seek(offset)
        return f.readline()

    def write_file(self, filename, buf, offset, nbytes):
        pass

    def heart_beat(self):
        return True
