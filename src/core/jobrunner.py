import thread
from Queue import Queue
from core.job import Job
from utils.conf_loader import load_config
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

    def add_task(self, context):
        self.tasks.put(context)

    def report_mapper_fail(self, jobid, taskid):
        pass

    def report_mapper_succ(self, jobid, taskid):
        pass

    def submit_job(self, jobconf):
        job = Job(self.jobid, jobconf, self)
        self.jobs[self.jobid] = job
        self.jobid += 1
        thread.start_new_thread(job.run, tuple())
        return job.id

    def serve(self):
        self.ns = locateNS(**self.conf['pyroNS'])

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.conf['namenode'])

        daemon = setup_Pyro_obj(self, self.ns)
        daemon.requestLoop()
