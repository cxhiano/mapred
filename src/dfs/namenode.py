""" Implementation of name node in distributed file system and management
command line.

This module depends on Pyro4. The name node will be run as a remote object
server. It can be accessed through Pyro name server.

When running this module, the name node will run in backgroung and a command
line will be started served as the management tool for the name node.
"""
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

def _random_pick(lst):
    """ Randomly pick an object from the lst """

    choice = random.randint(0, len(lst) - 1)
    return lst[choice]

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
    def create_file(self, filename):
        """ Create file with give filename

        According to REPLICATION_LEVEL, several replicas will be created in the
        distributed file systems across data nodes. If the number of data nodes
        if smaller than REPLICATION_LEVEL, the file creation will fail
        """
        if filename in self.files:
            raise IOError('File %s already exists!' % filename)

        datanodes = self.datanodes.keys()
        if len(datanodes) < REPLICATION_LEVEL:
            raise IOError('Too few datanodes! Cannot create %d replicas' %
                REPLICATION_LEVEL)

        replica_location = []
        for i in range(REPLICATION_LEVEL):
            while True:
                nodename = _random_pick(datanodes)
                if nodename not in replica_location:
                    break
            replica_location.append(nodename)
            node = self.datanodes[nodename]
            node.create_file(filename)

        self.files[filename] = replica_location
        logging.info('file %s created on %s' % (filename, str(replica_location)))

    @synchronized_method('__lock__')
    def delete_file(self, filename):
        """ Delete file with give filename """
        if not filename in self.files:
            raise IOError('File %s Not Found' % filename)

        for nodename in self.files[filename]:
            node = self.datanodes[nodename]
            node.delete_file(filename)
        del self.files[filename]
        logging.info('file %s deleted' % filename)

    @synchronized_method('__lock__')
    def get_file(self, filename):
        """ Get the data node with given file name """
        if not filename in self.files:
            raise IOError('File %s Not Found' % filename)

        nodename = _random_pick(self.files[filename])
        return self.datanodes[nodename]

    @synchronized_method('__lock__')
    def write_file(self, filename, offset, buf):
        if not filename in self.files:
            raise IOError('File %s Not Found' % filename)

        for nodename in self.files[filename]:
            node = self.datanodes[nodename]
            node.write_file(filename, offset, buf)

        return len(buf)

    @synchronized_method('__lock__')
    def read_file(self, filename, offset, nbytes):
        if not filename in self.files:
            raise IOError('File %s Not Found' % filename)

        node = self.get_file(filename)
        return node.read_file(filename, offset, nbytes)

    @synchronized_method('__lock__')
    def readline_file(self, filename, offset):
        if not filename in self.files:
            raise IOError('File %s Not Found' % filename)

        node = self.get_file(filename)
        return node.readline_file(filename, offset)

    @synchronized_method('__lock__')
    def close_file(self, filename):
        if not filename in self.files:
            raise IOError('File %s Not Found' % filename)

        for nodename in self.files[filename]:
            node = self.datanodes[nodename]
            node.close_file(filename)

    @synchronized_method('__lock__')
    def list_files(self):
        """ Return a list of files on this distributed file system """
        ret = []
        for fname in self.files:
            ret.append('filename: %s\t replica locations: %s' %
                (fname, ','.join(self.files[fname])))
        return ret

    @synchronized_method('__lock__')
    def list_nodes(self):
        """ Return a list of name of active data nodes """
        return self.datanodes.keys()

    @synchronized_method('__lock__')
    def check_datanodes(self):
        """ Check data nodes status, remove unhealthy data nodes and update
        file meta data

        Replica on unhealthy data nodes will be lost and if all replicas a file
        are lost, the file will be removed.
        """
        for nodename in self.datanodes.keys():
            datanode = self.datanodes[nodename]
            try:
                datanode.heartbeat()
            except Exception as e:
                del self.datanodes[nodename]
                logging.info('data node %s died: %s', nodename, e.message)
                for fname in self.files.keys():
                    replica_location = self.files[fname]
                    if nodename in replica_location:
                        replica_location.remove(nodename)
                        if len(replica_location) == 0:
                            logging.info('The last replica of %s lost' % fname)
                            del self.files[fname]

    def healthcheck(self):
        """ Periodically check data nodes status """
        while True:
            time.sleep(NAMENODE_HEALTH_CHECK_INTERVAL)
            self.check_datanodes()

if __name__ == '__main__':
    node = NameNode(sys.argv[1])
    node.start()
    cmd = CommandLine()

    cmd.register(
        'ls',
        print_list(node.list_files),
        'list all files')

    cmd.register(
        'rm',
        node.delete_file,
        'remove a file by name')

    cmd.register(
        'nodes',
        print_list(node.list_nodes),
        'list all data nodes')

    cmd.run()
