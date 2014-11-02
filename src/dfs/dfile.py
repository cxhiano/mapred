
class DFile:
    """ A file in distributed system """

    def __init__(self, name, datanode):
        self.name = name
        self.datanode = datanode

    def read(self, offset, nbytes):
        return self.datanode.read_file(self.name, offset, nbytes)

    def write(self, buf, offset, nbytes):
        pass

