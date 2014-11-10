""" This module provides functionality to maintain tasks for a job.

TaskList will provide uncompleted tasks until all tasks have been completed
"""
from Queue import Queue

FAILED = 0
SUCCEEDED = 1

class TaskList:
    """ Maintains a list of tasks

    The list is driven by messages. A FAILED message indicates that the task
    should be rerun and thus will be put back into the list. A SUCCEEDED message
    indicates that the task completed and will not be put back into the list.
    """
    def __init__(self, num_tasks):
        self.succeeds = 0
        self.fails = 0
        self.num_tasks = num_tasks
        self.msg = Queue()
        for taskid in range(num_tasks):
            self.msg.put((taskid, FAILED))

    def report_succeeded(self, taskid):
        """ Report a task completed """
        self.succeeds += 1
        self.msg.put((taskid, SUCCEEDED))

    def report_failed(self, taskid):
        """ Report a task failed. The task will be put back into the list """
        self.fails += 1
        self.msg.put((taskid, FAILED))

    def __iter__(self):
        """ Yield uncompleted tasks until all tasks finished """
        while self.succeeds < self.num_tasks:
            taskid, status = self.msg.get()
            if status == FAILED:
                yield taskid
