""" This module provides functionality to run a map task """

import sys
import logging
from core.task import Task
from mrio.collector import OutputCollector
from mrio.record_file import RecordFile
from utils.filenames import *

class MapTask(Task):
    def __init__(self, task_conf):
        super(MapTask, self).__init__(task_conf)
        self.name = 'map task %d for job %d' % (self.taskid, self.jobid)
        self.out_files = []

    def run(self):
        """ Run a map task

        Feed lines into mapper, collect output and write to corresponding
        partition
        """
        datanode = None
        out_files = []

        for i in range(self.cnt_reducers):
            fname = map_output(self.jobid, self.taskid, i)
            out_files.append(RecordFile(fname, self.namenode))

        input_ = RecordFile(self.input, self.namenode)
        out = OutputCollector(out_files)

        line_num = 0
        for line in input_:
            self.mapper(line_num, line, out)
            line_num += 1

        out.flush()

        for file_ in out_files:
            file_.close()

    def cleanup(self):
        pass
