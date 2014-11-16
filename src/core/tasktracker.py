""" This module provides functionality to monitor task running """
import sys
import logging
import thread
import Pyro4
from multiprocessing import Queue, Process
from core.conf import TASK_RUNNER_TIMEOUT
from utils.rmi import retrieve_object

COMPLETE = 0
FAILED = 1
KILL = 2
TIMEOUT = 3

class TaskTracker(object):
    """ Task tracker is responsible for running and monitoring of a single task

    Task tracker will launch a process to run task and monitor its status by
    using a multiprocess queue. When task completed or aborted due to errors,
    it will put a flag into the queue so that the tracker on the other side
    of the queue can know what happened. Tracker will deem a task as timeout
    when it have not got anything from the queue for some among of time.
    """
    def __init__(self, task, jobrunner, callback):
        self.task = task
        ns = Pyro4.locateNS()
        self.jobrunner = retrieve_object(ns, jobrunner)
        self.callback = callback
        self.message = Queue()
        self.runner = Process(target=self.run_task)

    def start_task(self):
        self.runner.start()

    def run_task(self):
        """ Run task and put flag into queue upon success or error """
        try:
            self.task.run()
            self.message.put(COMPLETE)
        except Exception as e:
            logging.info('Error when running %s: %s' % (self.task.name,
                e))
            self.message.put(FAILED)

    def kill_task(self):
        self.runner.terminate()
        self.message.put(KILL)

    def start_track(self):
        thread.start_new_thread(self.track, tuple())

    def track(self):
        """ Try to get flag put by the task from the queue, timeout if tracker
        cannot get anything from the queue for TASK_RUNNER_TIMEOUT seconds.
        """
        try:
            code = self.message.get(timeout=TASK_RUNNER_TIMEOUT)
        except:
            logging.info('%s timeout' % self.task.name)
            self.runner.terminate()
            code = TIMEOUT

        self.callback(self.task.jobid, self.task.taskid)

        if code == COMPLETE:
            logging.info('%s completed' % self.task.name)
            self.jobrunner.report_task_succeed(self.task.jobid, self.task.taskid)
        if code == KILL:
            logging.info('%s killed' % self.task.name)

        if code != COMPLETE:
            logging.info('%s failed' % self.task.name)
            self.jobrunner.report_task_fail(self.task.jobid, self.task.taskid)
