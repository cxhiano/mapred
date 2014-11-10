import os
import sys
import logging
import shutil
from core.task import Task
from mrio.record_file import RecordFile, record_iter
from mrio.collector import OutputCollector
from utils.sortfiles import sort_files
from utils.filenames import *

class ReduceTask(Task):
    def __init__(self, task_conf):
        super(ReduceTask, self).__init__(task_conf)

        self.name = 'reduce task %d for job %d' % (self.taskid, self.jobid)
        self.tmpdir = '%s/%s' % (self.tmpdir, reduce_input(self.jobid, self.taskid))
        self.output_fname = '%s.%s' % (self.output_dir, reduce_output(self.jobid,
            self.taskid))

    def run(self):
        try:
            os.mkdir(self.tmpdir)
        except OSError:
            logging.info('%s cannot create dir %s: %s' % (self.name, self.tmpdir,
                sys.exc_info()[1]))

        try:
            inputs = [RecordFile(fname, self.namenode) for fname in \
                self.inputs]
            reduce_input = sort_files(inputs, self.tmpdir)
            output_file = RecordFile(self.output_fname, self.namenode)
            out = OutputCollector([output_file])

            prev_key = None
            values = []
            for key, value in record_iter(reduce_input):
                if not prev_key is None and key != prev_key:
                    self.reducer(prev_key, values, out)
                    values = []
                values.append(value)
                prev_key = key

            if len(values) > 0:
                self.reducer(prev_key, values, out)

            out.flush()
            output_file.close()
        except:
            logging.info('%s error when running: %s' % (self.name,
                sys.exc_info()[1]))
            return False

        logging.info('%s completed' % self.name)

        self.jobrunner.report_task_succeed(self.jobid, self.taskid)

        try:
            shutil.rmtree(self.tmpdir)
        except OSError:
            logging.warning('%s: remove tmp dir failed: %s' %
                (self.name, sys.exc_info()[1]))

        return True

    def fail(self):
        logging.info('%s failed' % self.name)
        self.cleanup()
        self.jobrunner.report_task_fail(self.jobid, self.taskid)

    def cleanup(self):
        try:
            shutil.rmtree(self.tmpdir)
        except OSError:
            logging.warning('%s: remove tmp dir failed: %s' %
                (self.name, sys.exc_info()[1]))
