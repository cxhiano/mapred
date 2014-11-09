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
import utils.serialize as serialize

def is_mapper_task(task_conf):
    return task_conf.has_key('mapper')

class TaskRunner(Configurable):
    def __init__(self, conf):
        super(TaskRunner, self).__init__(load_config(conf))
        self.config_pyroNS()

        Pyro4.config.SERIALIZER = "marshal"

        self.slots = Queue()
        for i in range(TASK_RUNNER_SLOTS):
            self.slots.put(True)

        self.tasks = {}
        self.__lock__ = threading.Lock()

        ns = Pyro4.locateNS()

        if ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(ns, self.namenode)
        self.jobrunner = retrieve_object(ns, self.jobrunner)

    @synchronized_method('__lock__')
    def kill_task(self, jobid, taskid):
        tracker = self.tasks.get((jobid, taskid))
        if tracker is None:
            logging.warning('kill_task: jobid %d taskid %d does not exist' %
                (jobid, taskid))
            return
        tracker.kill_task()

    @synchronized_method('__lock__')
    def reap_task(self, task):
        self.slots.put(True)
        del self.tasks[(task.jobid, task.taskid)]

    def serve(self):
        while True:
            token = self.slots.get()
            task_conf = serialize.loads(self.jobrunner.get_task())
            logging.info('Got task with config %s, run on slot %d' %
                (str(task_conf), token))

            task_conf['namenode'] = self.namenode
            task_conf['jobrunner'] = self.jobrunner.get_name()

            if is_mapper_task(task_conf):
                task = MapTask(task_conf)
            else:
                task_conf['tmpdir'] = self.tmpdir
                task = ReduceTask(task_conf)

            tracker = TaskTracker(task, self.reap_task)

            with self.__lock__:
                self.tasks[(task.jobid, task.taskid)] = tracker
                tracker.start_track()
                tracker.start_task()
