import sys
import traceback
import thread
import random
import threading
import Pyro4
from dfs.conf import *
from core.configurable import Configurable
from utils.conf_loader import load_config
from utils.sync import synchronized_method
from utils.rmi import *
from utils.cmd import *

class NameNode(Configurable):
    """ The name node of distributed file system """

    def __init__(self, conf):
        super(NameNode, self).__init__(load_config(conf))
        self.config_pyroNS()

        self.datanodes = {}
        self.files = {}
        self.__lock__ = threading.RLock()

    def report(self, datanode):
        self.datanodes[datanode] = retrieve_object(self.ns, datanode)
        logging.info('receive report from %s' % datanode)
        return True

    def start(self):
        thread.start_new_thread(Pyro4.naming.startNSloop, tuple())

        self.ns = Pyro4.locateNS()
        if self.ns == None:
            logging.error('Cannot locate Pyro NS.')
            return

        daemon = export(self)
        thread.start_new_thread(daemon.requestLoop, tuple())
        logging.info('%s started' % self.name)

    @synchronized_method('__lock__')
    def create_file_meta(self, filename, datanode):
        if filename in self.files:
            raise IOError('File already exists!')
        self.files[filename] = datanode

    @synchronized_method('__lock__')
    def delete_file_meta(self, filename):
        if not filename in self.files:
            raise IOError('File not found!')
        del self.files[filename]

    def create_file(self, filename, preference=None):
        with self.__lock__:
            if filename in self.files:
                raise IOError('File %s already exists!' % filename)

        if preference is None:
            n = len(self.datanodes)
            if n == 0:
                raise IOError('No data node available')
                return
            datanode = self.datanodes.values()[random.randint(0, n - 1)]
        else:
            datanode = self.datanodes.get(preference)
            if datanode is None:
                raise IOError('Preferred data node not exist')
                return

        datanode.create_file(filename)

        return datanode

    def delete_file(self, filename):
        with self.__lock__:
            if not filename in self.files:
                logging.warning('%s does not exist' % filename)
                return

        self.files[filename].delete_file(filename)

    @synchronized_method('__lock__')
    def get_file(self, filename):
        if not filename in self.files:
            raise IOError('File Not Found')

        return self.files[filename]

    @synchronized_method('__lock__')
    def list_files(self):
        return self.files.keys()

    @synchronized_method('__lock__')
    def list_nodes(self):
        return self.datanodes.keys()

    def health_check(self):
        # TODO
        for datanode in self.datanodes.values():
            try:
                node.heart_beat()
            except:
                traceback.print_exc()

if __name__ == '__main__':
    node = NameNode(sys.argv[1])
    node.start()
    cmd = CommandLine()

    cmd.register(
        'files',
        print_list(node.list_files),
        'get a list of all files')

    cmd.register(
        'delete',
        node.delete_file,
        'delete specified file')

    cmd.register(
        'nodes',
        print_list(node.list_nodes),
        'get a list of all data nodes')

    cmd.run()
