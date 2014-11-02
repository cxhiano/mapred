import Pyro4

class DataNode:
    """ A data node in distributed file system """

    def run(self):
        pass

    def create_file(self, filename):
        pass

    def delete_file(self, filename):
        pass

    def read_file(self, filename, offset, bytes):
        pass

    def write_file(self, buf, offset, bytes):
        pass

