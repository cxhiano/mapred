import sys
import traceback
import thread
import random
import logging
import Pyro4
from utils.conf_loader import load_config
from utils.rmi import *
from .conf import *

class NameNode:
    """ The name node of distributed file system """

    def __init__(self, conf):
        self.conf = load_config(conf)
        self.datanodes = {}
        self.files = {}

    def get_conf(self, key):
        return self.conf[key]

    def run_pyro_naming_server(self):
        pyroNS_conf = self.conf['pyroNS']
        Pyro4.naming.startNSloop(pyroNS_conf['host'], int(pyroNS_conf['port']))

    def report(self, datanode):
        self.datanodes[datanode] = retrieve_object(self.ns, datanode)
        logging.info('receive report from %s' % datanode)
        return True

    def run(self):
        thread.start_new_thread(self.run_pyro_naming_server, tuple())

        self.ns = locateNS(**self.conf['pyroNS'])
        if self.ns == None:
            logging.error('Cannot locate Pyro NS.')
            return

        daemon = setup_Pyro_obj(self, self.ns)
        daemon.requestLoop()

    def create_file(self, filename, preference=None):
        if not self.files.get(filename) is None:
            raise IOError('File already exists!')

        if preference is None:
            n = len(self.datanodes)
            if n == 0:
                raise IOError('No data node available')
                return
            datanode = self.datanodes.values()[random.randint(0, n - 1)]
        else:
            if self.datanodes.get(preference.get_conf('name')) is None:
                raise IOError('Preferred data node not exist')
                return
            datanode = preference

        self.files[filename] = datanode
        datanode.create_file(filename)

        return datanode

    def delete_file(self, filename):
        datanode = self.files.get(filename)
        if datanode is None:
            raise IOError('File not found')

        datanode.delete_file(filename)

        return datanode

    def get_file(self, filename):
        datanode = self.files.get(filename)
        if datanode is None:
            raise IOError('File Not Found')

        return datanode

    def health_check(self):
        # TODO
        for datanode in self.datanodes.values():
            try:
                node.heart_beat()
            except:
                traceback.print_exc()
