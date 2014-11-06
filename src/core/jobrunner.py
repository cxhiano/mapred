from queue import Queue
from utils.filenames import *
from utils.splitter import Splitter
from utils.conf_loader import load_config
from mrio.record_file import RecordFile

class JobRunner:
    def __init__(self, conf):
        self.jobs = Queue()
        self.conf = load_config(conf)
        self.jobid = 1

        self.ns = locateNS(**self.conf['pyroNS'])

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.conf['namenode'])

    def add_job(self, job):
        self.jobs.put(job)

    def split_input(self, inputs):
        splitter = Splitter(5)
        results = []
        input_files = []
        for fname in inputs:
            input_files = self.namenode.create_file(map_input

    def generate_map_tasks(self):
        pass

    def serve(self):
        while True:
            job = self.jobs.get()
            # split
            # generate and dispatch mapper
            # generate and dispatch reducer


