import sys
import logging
import thread
from multiprocessing import Queue, Process
from core.conf import TASK_TIMEOUT

COMPLETE = 0
FAILED = 1
KILL = 2
TIMEOUT = 3

class TaskTracker(object):
    def __init__(self, task, callback):
        self.task = task
        self.callback = callback
        self.reporter = Queue()
        self.runner = Process(target=self.run_task)

    def start_task(self):
        self.runner.start()

    def run_task(self):
        if self.task.run():
            self.reporter.put(COMPLETE)
        else:
            self.reporter.put(FAILED)

    def kill_task(self):
        self.runner.terminate()
        self.reporter.put(KILL)

    def start_track(self):
        thread.start_new_thread(self.track, tuple())

    def track(self):
        try:
            code = self.reporter.get(timeout=TASK_TIMEOUT)
        except:
            logging.info('%s timeout' % self.task.name)
            self.runner.terminate()
            code = TIMEOUT

        self.callback(self.task.jobid, self.task.taskid)

        if code == COMPLETE:
            logging.info('%s exits normally' % self.task.name)
        if code == KILL:
            logging.info('%s killed' % self.task.name)

        if code != COMPLETE:
            self.task.fail()
