import Pyro4

class NameNode:
    """The name node of distributed file system """

    def run(self):
        daemon = Pyro4.Daemon()
        ns = Pyro4.locateNS()

    def create_file(self, filename):
        pass

    def delete_file(self, filename):
        pass

    def get_file(self, filename):
        pass

if '__name__' == '__main__':
    pass