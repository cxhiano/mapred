import os
import sys
import logging
import shutil
from core.task import Task
from mrio.record_file import RecordFile
from mrio.record_reader import record_iter
from mrio.collector import OutputCollector
from utils.sortfiles import sort_files
from utils.filenames import *

class ReduceTask(Task):
    def __init__(self, task_conf):
        super(ReduceTask, self).__init__(task_conf)
        self.name = 'reduce task %d for job %d' % (self.taskid, self.jobid)

    def setup(self):
        self.tmpdir = '%s/%s' % (self.tmpdir, reduce_input(self.jobid, self.taskid))

        try:
            os.mkdir(self.tmpdir)
        except OSError:
            logging.info('%s cannot create dir %s: %s' % (self.name, self.tmpdir,
                sys.exc_info()[1]))

        self.output_fname = '%s.%s' % (self.output_dir, reduce_output(self.jobid,
            self.taskid))

        self.namenode.create_file(self.output_fname)

    def run(self):
        try:
            self.setup()
        except:
            logging.info('%s setup failed' % self.name)
            self.fail()
            return

        try:
            inputs = [RecordFile(fname, self.namenode) for fname in \
                self.inputs]
            reduce_input = sort_files(inputs, self.tmpdir)
            output_file = RecordFile(self.output_fname, self.namenode)
            out = OutputCollector([output_file], 1)

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
            self.fail()
            return

        logging.info('%s completed' % self.name)

        self.jobrunner.report_reducer_succeed(self.jobid, self.taskid)

        try:
            shutil.rmtree(self.tmpdir)
        except OSError:
            logging.warning('%s: remove tmp dir failed: %s' %
                (self.name, sys.exc_info()[1]))

    def fail(self):
        logging.info('%s failed' % self.name)
        self.cleanup()
        self.jobrunner.report_reducer_fail(self.jobid, self.taskid)

    def cleanup(self):
        try:
            self.namenode.delete_file(self.output_fname)
        except IOError:
            logging.warning('%s: Error deleting file %s in cleanup: %s' %
                (self.name, self.output_fname, sys.exc_info()[1]))

        try:
            shutil.rmtree(self.tmpdir)
        except OSError:
            logging.warning('%s: remove tmp dir failed: %s' %
                (self.name, sys.exc_info()[1]))
