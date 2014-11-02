import Pyro4
import utils
import dfile
import sys
import traceback

class File:
    def __init__(self, nodename, filename):
        self.nodename = nodename
        self.filename = filename

class NameNode:
    """ The name node of distributed file system """

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

    def get_file(self, filename):
        f = self.files.get(filename)
        if f is None:
            return False
        datanode = utils.retrieve_object(f.nodename)
        return dfile.DFile(f.filename, datanode)

    def health_check(self):
        pass
        '''
        for info in self.conf['datanodes']:
            print info
            node = utils.retrieve_object(info['name'])

            try:
                node.heart_beat()
            except:
                traceback.print_exc()
        '''


if __name__ == '__main__':
    node = NameNode('name_node.xml')
    f = File('DataNode1', 'data_node.xml')
    node.files = { 'xml': f }
    node.health_check()
    '''
    f = node.get_file('xml')
    print f.read(0, 100)
    '''
