import threading
import logging
from Queue import Queue

FAILED = 0
SUCCEEDED = 1

def synchronized(func):
    def sync_func(*args, **kwargs):
        with threading.Lock():
            return func(*args, **kwargs)
    return sync_func

class TaskTracker:
    def __init__(self, num_tasks):
        self.succeeds = 0
        self.fails = 0
        self.num_tasks = num_tasks
        self.msg = Queue()
        for taskid in range(num_tasks):
            self.msg.put((taskid, FAILED))

    @synchronized
    def report_succeeded(self, taskid):
        self.succeeds += 1
        self.msg.put((taskid, SUCCEEDED))

    @synchronized
    def report_failed(self, taskid):
        self.fails += 1
        self.msg.put((taskid, FAILED))

    def __iter__(self):
        while self.succeeds < self.num_tasks:
            taskid, status = self.msg.get()
            if status == FAILED:
                yield taskid
