import Pyro4
from utils.conf import load_config
from utils.rmi import retrieve_object
import sys
import traceback
import thread

class NameNode:
    """ The name node of distributed file system """

    def __init__(self, conf):
        self.conf = load_config(conf)
        self.files = {}

    def run_pyro_naming_server(self):
        Pyro4.naming.startNSloop()

    def run(self):
        thread.start_new_thread(self.run_pyro_naming_server, tuple())

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
        nodename = self.files.get(filename)
        if nodename is None:
            raise IOError('File Not Found')
        return retrieve_object(nodename)

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
