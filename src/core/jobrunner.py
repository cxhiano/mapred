import thread
import threading
import Pyro4
from Queue import Queue
from core.job import Job
from core.conf import *
from core.configurable import *
from utils.conf_loader import load_config
from utils.rmi import *
from utils.sync import synchronized_method
import utils.serialize as serialize

class JobRunner(Configurable):
    def __init__(self, conf):
        super(JobRunner, self).__init__(load_config(conf))
        self.config_pyroNS()

        self.tasks = Queue(JOB_RUNNER_SLOTS)
        self.jobid = 1
        self.jobs = {}
        self.lock = threading.Lock()

    def get_name(self):
        return self.name

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
            logging.error('Receive mapper succeed report with unknown \
                jobid %d' % jobid)
            return
        logging.info('map task %d for job %d succeeded' % (taskid, jobid))
        job.report_mapper_succeed(taskid)

    def report_reducer_fail(self, jobid, taskid):
        job = self.jobs.get(jobid)
        if job is None:
            logging.error('Receive reducer failed report with unknown \
                jobid %d' % jobid)
            return
        logging.info('reduce task %d for job %d failed' % (taskid, jobid))
        job.report_mapper_fail(taskid)

    def report_reducer_succeed(self, jobid, taskid):
        job = self.jobs.get(jobid)
        if job is None:
            logging.error('Receive reducer succeeded report with unknown \
                jobid %d' % jobid)
            return
        logging.info('reduce task %d for job %d succeeded' % (taskid, jobid))
        job.report_mapper_succeed(taskid)

    @synchronized_method('lock')
    def report_job_succeed(self, jobid):
        job = self.jobs.get(jobid)
        if job is None:
            logging.error('Receive job succeded report with unknown jobid %d' \
                % jobid)
        logging.info('job %d completed' % jobid)
        del self.jobs[jobid]

    @synchronized_method('lock')
    def report_job_fail(self, jobid):
        job = self.jobs.get(jobid)
        if job is None:
            logging.error('Receive job fail report with unknown jobid %d' \
                % jobid)
        logging.info('job %d failed' % jobid)
        del self.jobs[jobid]

    @synchronized_method('lock')
    def submit_job(self, jobconf):
        try:
            job = Job(self.jobid, serialize.loads(jobconf), self)
        except ValidationError as e:
            logging.info('Invalid job config: %s' % e.msg)
            return -1

        self.jobs[self.jobid] = job
        self.jobid += 1
        thread.start_new_thread(job.run, tuple())
        return job.id

    def serve(self):
        self.ns = Pyro4.locateNS()

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.namenode)

        daemon = export(self)
        daemon.requestLoop()
