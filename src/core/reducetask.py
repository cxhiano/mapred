""" This module provides functionality to run a map task """

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
        """ Run a reduce task

        First create a temporary directory for merge sort. Then merge sort all
        input files and feed records to reducer
        """
        try:
            os.mkdir(self.tmpdir)
        except OSError as e:
            logging.info('%s cannot create dir %s: %s' % (self.name, self.tmpdir,
                e.message))

        reduce_input = sort_files(self.inputs, self.tmpdir, self.namenode)
        logging.info('reduce task: sorting file done!')
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

        logging.info('%s completed' % self.name)

        try:
            shutil.rmtree(self.tmpdir)
        except OSError as e:
            logging.warning('%s: remove tmp dir failed: %s' % (self.name,
                e.message))

    def cleanup(self):
        try:
            shutil.rmtree(self.tmpdir)
        except OSError:
            logging.warning('%s: remove tmp dir failed: %s' %
                (self.name, sys.exc_info()[1]))
