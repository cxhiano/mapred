from Queue import Queue
from utils.filenames import *
from utils.splitter import Splitter
from utils.conf_loader import load_config
from mrio.record_file import RecordFile

class JobRunner:
    def __init__(self, conf):
        self.jobs = Queue()
        self.conf = load_config(conf)
        self.jobid = 1

    def add_job(self, job):
        self.jobs.put(job)

    def split_input(self, job):
        splitter = Splitter(5)
        results = []
        input_files = []
        for fname in job.inputs:
            input_files.append(RecordFile(fname, self.namenode))

        taskid = 0
        for block in splitter.split(input_files):
            fname = map_input(job.id, taskid)
            taskid += 1
            datanode = self.namenode.create_file(fname)
            for record in block:
                datanode.write_file(fname, record)
            datanode.close_file(fname)
            results.append(fname)

        return results

    def generate_map_tasks(self):
        pass

    def serve(self):
        self.ns = locateNS(**self.conf['pyroNS'])

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.conf['namenode'])

        while True:
            job = self.jobs.get()
            job.id = self.jobid
            self.jobid += 1
            blocks = self.split_input(job)
            for i in range(len(blocks)):
                pass

            # generate and dispatch reducer
