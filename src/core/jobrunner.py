import thread
from Queue import Queue
from core.job import Job
from core.context import Context
from utils.filenames import *
from utils.splitter import Splitter
from utils.conf_loader import load_config
from mrio.record_file import RecordFile
from utils.rmi import *

class JobRunner:
    def __init__(self, conf):
        self.tasks = Queue(2)
        self.conf = load_config(conf)
        self.jobid = 1
        self.jobs = {}

    def get_task(self):
        context = self.tasks.get()
        return context.serialize()

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

    def make_mapper_context(self, taskid, input_fn, job):
        context = Context()
        context.jobid = job.id
        context.taskid = taskid
        context.mapper = job.mapper
        context.cnt_reducers = job.cnt_reducers
        context.input = input_fn
        return context

    def run_job(self, job):
        logging.info('start running job %d' % job.id)
        blocks = self.split_input(job)
        for i in range(len(blocks)):
            context = self.make_mapper_context(i, blocks[i], job)
            logging.info('enqueue task %d for job %d' % (i, job.id))
            self.tasks.put(context)

        # generate and dispatch reducer

    def report_mapper_fail(self, jobid, taskid):
        pass

    def report_mapper_succ(self, jobid, taskid):
        pass

    def submit_job(self, jobconf):
        job = Job(jobconf)
        job.id = self.jobid
        self.jobid += 1
        thread.start_new_thread(self.run_job, tuple([job]))
        return job.id

    def serve(self):
        self.ns = locateNS(**self.conf['pyroNS'])

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.conf['namenode'])

        daemon = setup_Pyro_obj(self, self.ns)
        daemon.requestLoop()
