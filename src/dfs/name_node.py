import Pyro4
import utils

class NameNode:
    """ The name node of distributed file system """

    def __init__(self, conf):
        self.conf = utils.load_config(conf)

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

    def get_file(self, filename):
        pass

if __name__ == '__main__':
    node = NameNode('name_node.xml')
