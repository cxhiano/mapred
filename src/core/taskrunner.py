""" Implementation of a task runner that runs map tasks and reduce tasks

A task runner has several slots for task running. It get task config from job
runner and create a task accordingly. Then launch a process to run the task.
Task runner also monitor the task running and report timeout and failure
"""
import thread
import threading
import Pyro4
from multiprocessing import Queue
from core.conf import *
from core.configurable import Configurable
from core.maptask import MapTask
from core.reducetask import ReduceTask
from core.tasktracker import TaskTracker
from utils.rmi import *
from utils.conf_loader import load_config
from utils.sync import synchronized_method
from utils.cmd import *
import utils.serialize as serialize

def is_mapper_task(task_conf):
    return task_conf.has_key('mapper')

class TaskRunner(Configurable):
    def __init__(self, conf):
        super(TaskRunner, self).__init__(load_config(conf))
        self.config_pyroNS()

        self.slots = Queue()
        for i in range(TASK_RUNNER_SLOTS):
            self.slots.put(True)

        self.tasks = {}
        self.__lock__ = threading.Lock()

        self.ns = Pyro4.locateNS()

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.namenode)
        self.jobrunner = retrieve_object(self.ns, self.jobrunner)

    @synchronized_method('__lock__')
    def kill_task(self, jobid, taskid):
        jobid = int(jobid)
        taskid = int(taskid)
        tracker = self.tasks.get((jobid, taskid))
        if tracker is None:
            logging.warning('kill_task: jobid %d taskid %d does not exist' %
                (jobid, taskid))
            return
        tracker.kill_task()

    @synchronized_method('__lock__')
    def reap_task(self, jobid, taskid):
        """ When a task completed or failed or timeout, empty its slot for new
        task.
        """
        self.slots.put(True)
        del self.tasks[(jobid, taskid)]

    @synchronized_method('__lock__')
    def list_tasks(self):
        ret = []
        for tracker in self.tasks.values():
            ret.append(tracker.task.name)

        return ret

    def run(self):
        """ The main routine of task runner

        When there are available slots, task runner will try to get task from
        job runner and launch a process to run each task. Tasks will be monitor
        by task trackers.
        """
        Pyro4.config.SERIALIZER = 'marshal'
        while True:
            token = self.slots.get()
            task_conf = serialize.loads(self.jobrunner.get_task(self.name))
            logging.info('Got task with config %s' % str(task_conf))

            task_conf['namenode'] = self.namenode

            if is_mapper_task(task_conf):
                task = MapTask(task_conf)
            else:
                task_conf['tmpdir'] = self.tmpdir
                task = ReduceTask(task_conf)

            tracker = TaskTracker(task, self.jobrunner.get_name(), self.reap_task)

            with self.__lock__:
                if (task.jobid, task.taskid) in self.tasks:
                    logging.info('%s already running' % task.name)
                else:
                    self.tasks[(task.jobid, task.taskid)] = tracker
                    tracker.start_track()
                    tracker.start_task()

    def start(self):
        """ Export self as remote object and start main routine """
        daemon = export(self)
        thread.start_new_thread(daemon.requestLoop, tuple())

        thread.start_new_thread(self.run, tuple())
        logging.info('%s started' % self.name)

    def heartbeat(self):
        return True

if __name__ == '__main__':
    taskrunner = TaskRunner(sys.argv[1])
    taskrunner.start()

    cmd = CommandLine()

    cmd.register(
        'kill',
        taskrunner.kill_task,
        'kill task identified by given jobid and taskid')

    cmd.register(
        'tasks',
        print_list(taskrunner.list_tasks),
        'list running tasks')

    cmd.run()
