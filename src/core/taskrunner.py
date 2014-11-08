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
from utils.conf_loader import load_config
import utils.serialize as serialize

def is_mapper_task(task_conf):
    return task_conf.has_key('mapper')

class TaskRunner(Configurable):
    def __init__(self, conf):
        self.load_dict(load_config(conf))
        Pyro4.config.SERIALIZER = "marshal"
        self.lock = threading.Lock()

    def run_maptask(self, task_conf):
        jobrunner = retrieve_object(self.ns, self.jobrunner)
        jobid = task_conf['jobid']
        taskid = task_conf['taskid']
        maptask = MapTask(task_conf, self)

        try:
            maptask.run()
        except:
            logging.info('map task %d for job %d failed: %s' % \
                (taskid, jobid, sys.exc_info()[1]))

            self.jobrunner.report_mapper_fail(jobid, taskid)

            return

        logging.info('map task %d for job %d completed' % \
            (taskid, jobid))

        jobrunner.report_mapper_succeed(jobid, taskid)

    def run_reducetask(self, task_conf):
        jobrunner = retrieve_object(self.ns, self.jobrunner)
        jobid = task_conf['jobid']
        taskid = task_conf['taskid']

        tmpdir = '%s/%s' % (self.tmpdir, reduce_input(jobid, taskid))
        try:
            os.mkdir(tmpdir)
        except OSError:
            logging.error('make tmp dir for reduce task %d job %d \
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
        self.ns = locateNS(self.pyroNS['host'], int(self.pyroNS['port']))

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.namenode)
        jobrunner = retrieve_object(self.ns, self.jobrunner)

        while True:
            task_conf = serialize.loads(jobrunner.get_task())
            logging.info('Got task with config %s' % str(task_conf))

            if is_mapper_task(task_conf):
                thread.start_new_thread(self.run_maptask, tuple([task_conf]))
            else:
                thread.start_new_thread(self.run_reducetask, tuple([task_conf]))
