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
    """ The name node of distributed file system

    Responsible for managing file meta data and coordinating data nodes
    """

    def __init__(self, conf):
        super(NameNode, self).__init__(load_config(conf))
        self.config_pyroNS()

        self.datanodes = {}
        self.files = {}
        self.__lock__ = threading.RLock()

    def report(self, datanode):
        """ Method for data nodes to report to name node when data nodes start
        functioning.

        When a data node comes online, it should call this method to report
        itself to name node.
        """
        self.datanodes[datanode] = retrieve_object(self.ns, datanode)
        logging.info('receive report from %s' % datanode)
        return True

    def start(self):
        """ Run name node in background.

        3 threads will be started, running remote object name server, name node
        server and health check routine respectively
        """
        thread.start_new_thread(Pyro4.naming.startNSloop, tuple())

        self.ns = Pyro4.locateNS()
        if self.ns == None:
            logging.error('Cannot locate Pyro NS.')
            return

        daemon = export(self)
        thread.start_new_thread(daemon.requestLoop, tuple())
        thread.start_new_thread(self.healthcheck, tuple())
        logging.info('%s started' % self.name)

    @synchronized_method('__lock__')
    def create_file_meta(self, filename, datanode):
        """ Create file meta data for given filename which corresponds to file
        on the given datanode
        """
        if filename in self.files:
            raise IOError('File already exists!')
        self.files[filename] = datanode

    @synchronized_method('__lock__')
    def delete_file_meta(self, filename):
        """ Delete file mete data for given filename """
        if not filename in self.files:
            raise IOError('File not found!')
        del self.files[filename]

    def create_file(self, filename, preference=None):
        """ Create file with give filename

        The parameter preference
        """
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
                logging.info('%s does not exist' % filename)
                return

        self.datanodes[self.files[filename]].delete_file(filename)

    @synchronized_method('__lock__')
    def get_file(self, filename):
        if not filename in self.files:
            raise IOError('File Not Found')

        return self.datanodes[self.files[filename]]

    @synchronized_method('__lock__')
    def list_files(self):
        return self.files.keys()

    @synchronized_method('__lock__')
    def list_nodes(self):
        return self.datanodes.keys()

    @synchronized_method('__lock__')
    def check_datanodes(self):
        for name in self.datanodes.keys():
            datanode = self.datanodes[name]
            try:
                datanode.heartbeat()
            except Exception as e:
                del self.datanodes[name]
                logging.info('data node %s died: %s', name, e.message)
                for fname in self.files.keys():
                    if self.files[fname] == name:
                        del self.files[fname]

    def healthcheck(self):
        while True:
            time.sleep(NAMENODE_HEALTH_CHECK_INTERVAL)
            self.check_datanodes()

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
