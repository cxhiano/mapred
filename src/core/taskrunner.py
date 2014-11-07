import sys
import os
import shutil
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

    def run_maptask(self, task_conf):
        jobid = task_conf['jobid']
        taskid = task_conf['taskid']
        maptask = MapTask(task_conf, self)

        try:
            maptask.run()
        except:
            logging.info('map task %d for job %d failed: %s' % \
                (taskid, jobid, sys.exc_info()[1]))

            self.jobrunner.report_mapper_fail(jobid, taskid)

        logging.info('map task %d for job %d completed' % \
            (taskid, jobid))

        self.jobrunner.report_mapper_succeed(jobid, taskid)

    def run_reducetask(self, task_conf):
        jobid = task_conf['jobid']
        taskid = task_conf['taskid']
        tmpdir = '%s/%s' % (self.tmpdir, reduce_input(jobid, taskid))
        try:
            os.mkdir(tmpdir)
        except OSError:
            logging.error('make tmp dir for reduce task %d job %d \
                failed: %s' % (taskid, jobid, sys.exc_info()[1]))
            return

        task_conf['tmpdir'] = tmpdir

        task_conf['output_fname'] = '%s.%s' % (task_conf['output_dir'], \
            reduce_output(jobid, taskid))
        try:
            self.namenode.create_file(task_conf['output_fname'])
        except IOError:
            logging.error('Error creating output file for reduce \
                task %d' % taskid)
            return

        reducetask = ReduceTask(task_conf, self)

        reducetask.run()
        '''
        except:
            logging.info('reduce task %d for job %d failed: %s' % \
                (reducetask.taskid, reducetask.jobid, \
                 sys.exc_info()[1]))

            self.jobrunner.report_reducer_fail(reducetask.jobid,  \
                reducetask.taskid)

        logging.info('reduce task %d for job %d completed' % \
            (reducetask.taskid, reducetask.jobid))
        '''

        self.jobrunner.report_reducer_succeed(reducetask.jobid, \
            reducetask.taskid)

        try:
            shutil.rmtree(tmpdir)
        except OSError:
            logging.error('remove tmp dir for reduce task %d job %d \
                failed: %s' % (task_conf['jobid'],  \
                               task_conf['taskid'], \
                               sys.exc_info()[1]))

    def serve(self):
        self.ns = locateNS(self.pyroNS['host'], int(self.pyroNS['port']))

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.namenode)
        self.jobrunner = retrieve_object(self.ns, self.jobrunner)

        while True:
            task_conf = serialize.loads(self.jobrunner.get_task())
            logging.info('Got task with config %s' % str(task_conf))

            if is_mapper_task(task_conf):
                self.run_maptask(task_conf)
            else:
                self.run_reducetask(task_conf)
