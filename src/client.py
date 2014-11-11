import Pyro4
from core.configurable import Configurable
from utils.rmi import retrieve_object
import utils.serialize as serialize

class Client(Configurable):
    def __init__(self, conf):
        super(Client, self).__init__(conf)
        self.config_pyroNS()
        ns  = Pyro4.locateNS()
        self.jobrunner = retrieve_object(ns, self.jobrunner)
        self.namenode = retrieve_object(ns, self.namenode)
        Pyro4.config.SERIALIZER = 'marshal'

    def submit(self, job_conf):
        self.jobrunner.submit_job(serialize.dumps(job_conf))

    def upload(self, fname, local_file):
        datanode = self.namenode.create_file(fname)
        datanode.write_file(fname, 0, local_file.read())
        datanode.close_file(fname)

    def download(self, fname, local_file):
        bytes_read = 0
        datanode = self.namenode.get_file(fname)
        while True:
            buf = datanode.read_file(fname, bytes_read, 1024)
            if nbytes == 0:
                break
            local_file.write(buf)
            bytes_read += len(buf)
        datanode.close_file(fname)
