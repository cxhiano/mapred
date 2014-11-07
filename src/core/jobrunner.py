import thread
from Queue import Queue
from core.job import Job
from utils.conf_loader import load_config
from utils.rmi import *
import utils.serialize as serialize

class JobRunner:
    def __init__(self, conf):
        self.tasks = Queue(2)
        self.conf = load_config(conf)
        self.jobid = 1
        self.jobs = {}

    def get_task(self):
        task_conf = self.tasks.get()
        return serialize.dumps(task_conf)

    def add_task(self, task_conf):
        self.tasks.put(task_conf)

    def report_mapper_fail(self, jobid, taskid):
        job = self.jobs.get(jobid)
        if job is None:
            logging.error('Receive mapper fail report with unknown jobid')
            return
        job.report_mapper_fail(taskid)

    def report_mapper_succeed(self, jobid, taskid):
        job = self.jobs.get(jobid)
        if job is None:
            logging.error('Receive mapper succeed report with unknown jobid')
            return
        job.report_mapper_succeed(taskid)

    def submit_job(self, jobconf):
        job = Job(self.jobid, serialize.loads(jobconf), self)
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
