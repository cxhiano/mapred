import time
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

        self.task_queue = Queue(JOB_RUNNER_SLOTS)
        self.running_tasks = {}
        self.task_runners = {}
        self.jobid = 1
        self.jobs = {}
        self.__lock__ = threading.RLock()

    def get_name(self):
        return self.name

    def get_task(self, task_runner):
        task_conf = self.task_queue.get()
        jobid, taskid = task_conf['jobid'], task_conf['taskid']
        with self.__lock__:
            self.running_tasks[(jobid, taskid)] = task_runner
            if not task_runner in self.task_runners:
                logging.info('receive get task request from new task runner %s'
                    % task_runner)
                self.task_runners[task_runner] = retrieve_object(self.ns,
                    task_runner)
        return serialize.dumps(task_conf)

    def add_task(self, task_conf):
        self.task_queue.put(task_conf)

    @synchronized_method('__lock__')
    def check_task_runners(self):
        for name in self.task_runners.keys():
            task_runner = self.task_runners[name]
            try:
                task_runner.heartbeat()
            except Exception as e:
                logging.info('%s does not response: %s', name, e.message)
                del self.task_runners[name]
                for jobid, taskid in self.running_tasks.keys():
                    if self.running_tasks[(jobid, taskid)] == name:
                        self.report_task_fail(jobid, taskid)

    def healthcheck(self):
        while True:
            time.sleep(HEARTBEAT_INTERVAL)
            self.check_task_runners()

    @synchronized_method('__lock__')
    def report_task_fail(self, jobid, taskid):
        job = self.jobs.get(jobid)
        if not (jobid, taskid) in self.running_tasks:
            logging.error('Receive task fail report with unknown jobid %d \
                taskid %d' % (jobid, taskid))
            return

        logging.info('job %d, task %d failed' % (jobid, taskid))
        del self.running_tasks[(jobid, taskid)]

        job.report_task_fail(taskid)

    @synchronized_method('__lock__')
    def report_task_succeed(self, jobid, taskid):
        job = self.jobs.get(jobid)
        if not (jobid, taskid) in self.running_tasks:
            logging.error('Receive task succeed report with unknown jobid %d \
                taskid %d' % (jobid, taskid))
            return

        logging.info('job %d, task %d succeded' % (jobid, taskid))
        del self.running_tasks[(jobid, taskid)]

        job.report_task_succeed(taskid)

    @synchronized_method('__lock__')
    def report_job_succeed(self, jobid):
        job = self.jobs.get(jobid)
        if job is None:
            logging.error('Receive job succeded report with unknown jobid %d' \
                % jobid)
        logging.info('job %d completed' % jobid)
        del self.jobs[jobid]

    @synchronized_method('__lock__')
    def report_job_fail(self, jobid):
        job = self.jobs.get(jobid)
        if job is None:
            logging.error('Receive job fail report with unknown jobid %d' \
                % jobid)
        logging.info('job %d failed' % jobid)
        del self.jobs[jobid]

    @synchronized_method('__lock__')
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
        thread.start_new_thread(self.healthcheck, tuple())
        daemon.requestLoop()
