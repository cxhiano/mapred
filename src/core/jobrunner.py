import thread
from Queue import Queue
from core.job import Job
from core.configurable import Configurable
from utils.conf_loader import load_config
from utils.rmi import *
import utils.serialize as serialize

class JobRunner(Configurable):
    def __init__(self, conf):
        self.load_dict(load_config(conf))
        self.tasks = Queue(2)
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
        logging.info('map task %d for job %d failed' % (taskid, jobid))
        job.report_mapper_fail(taskid)

    def report_mapper_succeed(self, jobid, taskid):
        job = self.jobs.get(jobid)
        if job is None:
            logging.error('Receive mapper succeed report with unknown jobid %d' % jobid)
            return
        logging.info('map task %d for job %d succeeded' % (taskid, jobid))
        job.report_mapper_succeed(taskid)

    def report_reducer_fail(self, jobid, taskid):
        logging.info('reduce task %d for job %d failed' % (taskid, jobid))

    def report_reducer_succeed(self, jobid, taskid):
        logging.info('reduce task %d for job %d succeeded' % (taskid, jobid))

    def submit_job(self, jobconf):
        job = Job(self.jobid, serialize.loads(jobconf), self)
        self.jobs[self.jobid] = job
        self.jobid += 1
        thread.start_new_thread(job.run, tuple())
        return job.id

    def serve(self):
        self.ns = locateNS(self.pyroNS['host'], int(self.pyroNS['port']))

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.namenode)

        daemon = export(self)
        daemon.requestLoop()
