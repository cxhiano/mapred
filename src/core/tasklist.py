""" This module provides functionality to maintain tasks for a job.

TaskList will provide uncompleted tasks until all tasks have been completed
"""
from threading import Lock
from Queue import Queue
from utils.sync import synchronized_method

FAILED = 0
SUCCEEDED = 1

class TaskList:
    """ Maintains a list of tasks

    The list is driven by messages. A FAILED message indicates that the task
    should be rerun and thus will be put back into the list. A SUCCEEDED message
    indicates that the task completed and will not be put back into the list.
    """
    def __init__(self, num_tasks):
        self.fails = 0
        self.num_tasks = num_tasks
        self.queue = Queue()
        self.in_queue = set()
        self.completed = set()
        for taskid in range(num_tasks):
            self.queue.put((taskid, FAILED))
            self.in_queue.add(taskid)
        self.__lock__ = Lock()

    @synchronized_method('__lock__')
    def report_succeeded(self, taskid):
        """ Report a task completed """
        if taskid not in self.completed:
            self.completed.add(taskid)
        if taskid not in self.in_queue:
            self.queue.put((taskid, SUCCEEDED))
            self.in_queue.add(taskid)

    @synchronized_method('__lock__')
    def report_failed(self, taskid):
        """ Report a task failed. The task will be put back into the list """
        self.fails += 1
        if taskid not in self.in_queue:
            self.queue.put((taskid, FAILED))
            self.in_queue.add(taskid)

    def next(self, timeout):
        """ Return an uncompleted tasks """
        while len(self.completed) < self.num_tasks:
            taskid, status = self.queue.get(timeout=timeout)
            self.in_queue.remove(taskid)
            if status == FAILED:
                return taskid

        return None
