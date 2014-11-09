import sys
import os
import shutil
import thread
import threading
import Pyro4
from Queue import Queue
from utils.rmi import *
from utils.filenames import *
from core.configurable import Configurable
from core.maptask import MapTask
from core.reducetask import ReduceTask
from core.conf import *
from utils.conf_loader import load_config
import utils.serialize as serialize

def _slot(method):
    def wrapper(self, *args, **kwargs):
        ret = method(self, *args, **kwargs)
        self.slots.put(True)
        return ret
    return wrapper

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

        self.ns = Pyro4.locateNS()

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.namenode)
        self.jobrunner = retrieve_object(self.ns, self.jobrunner)

    @_slot
    def run_reducetask(self, task_conf):
        jobrunner = retrieve_object(self.ns, self.jobrunner)
        jobid = task_conf['jobid']
        taskid = task_conf['taskid']

        tmpdir = '%s/%s' % (self.tmpdir, reduce_input(jobid, taskid))
        try:
            os.mkdir(tmpdir)
        except OSError:
            logging.warning('make tmp dir for reduce task %d job %d \
                failed: %s' % (taskid, jobid, sys.exc_info()[1]))

        task_conf['tmpdir'] = tmpdir

        task_conf['output_fname'] = '%s.%s' % (task_conf['output_dir'], \
            reduce_output(jobid, taskid))
        try:
            self.namenode.create_file(task_conf['output_fname'])
        except IOError:
            logging.error('Error creating output file for reduce \
                task %d' % taskid)

            jobrunner.report_reducer_fail(jobid, taskid)

            return

        reducetask = ReduceTask(task_conf, self)

        try:
            reducetask.run()
        except:
            logging.info('reduce task %d for job %d failed: %s' % \
                (taskid, jobid, sys.exc_info()[1]))

            reducetask.cleanup()

            jobrunner.report_reducer_fail(jobid, taskid)

            return

        logging.info('reduce task %d for job %d completed' % \
            (taskid, jobid))

        jobrunner.report_reducer_succeed(jobid, taskid)

        try:
            shutil.rmtree(tmpdir)
        except OSError:
            logging.error('remove tmp dir for reduce task %d job %d \
                failed: %s' % (jobid, taskid, sys.exc_info()[1]))

    def serve(self):
        while self.slots.get():
            task_conf = serialize.loads(self.jobrunner.get_task())
            logging.info('Got task with config %s' % str(task_conf))

            task_conf['namenode'] = self.namenode
            task_conf['jobrunner'] = self.jobrunner.get_name()

            if is_mapper_task(task_conf):
                task = MapTask(task_conf)
            else:
                break
                task = ReduceTask(task_conf)

            task.start()
