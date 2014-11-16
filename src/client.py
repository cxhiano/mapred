import sys
import Pyro4
from core.configurable import Configurable
from utils.rmi import retrieve_object
import utils.serialize as serialize
from utils.conf_loader import load_config

class Client(Configurable):
    def __init__(self, conf):
        super(Client, self).__init__(conf)
        self.config_pyroNS()
        ns  = Pyro4.locateNS()
        self.jobrunner = retrieve_object(ns, self.jobrunner)
        self.namenode = retrieve_object(ns, self.namenode)
        Pyro4.config.SERIALIZER = 'marshal'

    def submit(self, job_conf):
        '''Submit a job

        Example job_conf:
            'mapper': mapper function,
            'reducer': reducer function,
            'cnt_reducers': <integer>,
            'inputs': <list of input names>,
            'output_dir': <output directory name>
        '''
        self.jobrunner.submit_job(serialize.dumps(job_conf))

    def put(self, fname, local_file):
        '''Upload a file to dfs'''
        local_file = open(local_file, 'r')
        self.namenode.create_file(fname)
        self.namenode.write_file(fname, 0, local_file.read())
        self.namenode.close_file(fname)

    def get(self, fname, local_file):
        '''Download a file from dfs'''
        local_file = open(local_file, 'w')
        bytes_read = 0
        while True:
            buf = self.namenode.read_file(fname, bytes_read, 1024)
            if len(buf) == 0:
                break
            local_file.write(buf)
            bytes_read += len(buf)

def usage():
    print 'python client.py get <dfs_file_name> <local_file_name>'
    print 'python client.py put <dfs_file_name> <local_file_name>'

if __name__ == '__main__':
    if len(sys.argv) < 3:
        usage()
        sys.exit(0)

    client = Client(load_config('conf/client.xml'))
    if sys.argv[1] == 'get':
        client.get(sys.argv[2], sys.argv[3])
    elif sys.argv[1] == 'put':
        client.put(sys.argv[2], sys.argv[3])
    else:
        usage()
