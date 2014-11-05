import sys
import traceback
import thread
import Pyro4
from utils.conf_loader import load_config
from utils.rmi import *
from conf import *

class NameNode:
    """ The name node of distributed file system """

    def __init__(self, conf):
        self.conf = load_config(conf)
        self.datanodes = {}
        self.files = {}

    def run_pyro_naming_server(self):
        pyroNS_conf = self.conf['pyroNS']
        Pyro4.naming.startNSloop(pyroNS_conf['host'], int(pyroNS_conf['port']))

    def report(self, datanode):
        self.datanodes[datanode.conf['name']] = datanode
        return True

    def run(self):
        thread.start_new_thread(self.run_pyro_naming_server, tuple())

        ns = locateNS(**self.conf['pyroNS'])
        if ns == None:
            print 'Cannot locate Pyro NS.'
            return

        daemon = setup_Pyro_obj(self, self.conf['name'], self.conf['host'],
            int(self.conf['port']), ns)
        daemon.requestLoop()

    def create_file(self, filename, preference=None):
        if not self.files.get(filename) is None:
            raise IOError('File already exists!')

        if preference is None:
            pass
        else:
            self.file[filename] = preference.conf['name']
            datanode = preference

        datanode.create_file(filename)

        return datanode

    def delete_file(self, filename):
        nodename = self.files.get(filename)
        if nodename is None:
            raise IOError('File not found')

        datanode = self.datanodes[nodename]
        datanode.delete_file(filename)

        return datanode

    def get_file(self, filename):
        nodename = self.files.get(filename)
        if nodename is None:
            raise IOError('File Not Found')

        return self.datanodes[nodename]

    def health_check(self):
        for datanode in self.datanodes.values():
            try:
                node.heart_beat()
            except:
                traceback.print_exc()
