import Pyro4
import utils

class NameNode:
    """ The name node of distributed file system """

    def __init__(self, name):
        self.name = name

    def run(self):
        Pyro4.Daemon.serveSimple(
            {
                self: self.name
            },
            port=12345,
            ns=True)

    def create_file(self, filename):
        pass

    def delete_file(self, filename):
        pass

    def get_file(self, filename):
        pass

if '__name__' == '__main__':
    pass
