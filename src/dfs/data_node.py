import Pyro4
import utils

class DataNode:
    """ A data node in distributed file system """

    def __init__(self, conf):
        self.conf = utils.load_config(conf)
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
        try:
            f = open(filename, 'r')
            f.seek(offset)
            return f.read(nbytes)
        except IOError:
            return False

    def write_file(self, buf, offset, nbytes):
        pass

    def heart_beat(self):
        return True

if __name__ == '__main__':
    node = DataNode('data_node.xml')
    node.run()
