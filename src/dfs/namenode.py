import sys
import traceback
import thread
import random
import logging
import threading
import Pyro4
from core.configurable import Configurable
from dfs.conf import *
from utils.conf_loader import load_config
from utils.sync import synchronized_method
from utils.rmi import *

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

    def run(self):
        thread.start_new_thread(Pyro4.naming.startNSloop, tuple())

        self.ns = Pyro4.locateNS()
        if self.ns == None:
            logging.error('Cannot locate Pyro NS.')
            return

        daemon = export(self)
        daemon.requestLoop()

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

    def health_check(self):
        # TODO
        for datanode in self.datanodes.values():
            try:
                node.heart_beat()
            except:
                traceback.print_exc()
