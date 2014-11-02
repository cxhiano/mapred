import Pyro4
import utils

class DataNode:
    """ A data node in distributed file system """

    def __init__(self, name):
        self.name = name

    def run(self):
        Pyro4.Daemon.serveSimple(
            {
                self: self.name
            },
            port=54321,
            ns=True)

    def create_file(self, filename):
        pass

    def delete_file(self, filename):
        pass

    def read_file(self, filename, offset, bytes):
        pass

    def write_file(self, buf, offset, bytes):
        pass

if __name__ == '__main__':
    d = DataNode('yeah')
    d.run()
