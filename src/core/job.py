import types
import logging
from core.context import Context
from mrio.record_file import RecordFile
from utils.splitter import Splitter
from utils.filenames import *
import utils.serialize as serialize

class Job:
    def __init__(self, jobid, jobconf, jobrunner):
        self.id = jobid
        serialize.loads(self, jobconf)
        self.jobrunner = jobrunner

    def run(self):
        logging.info('start running job %d' % self.id)
        blocks = self.split_input()
        for i in range(len(blocks)):
            context = self.make_mapper_context(i, blocks[i])
            logging.info('enqueue task %d for job %d' % (i, self.id))
            self.jobrunner.add_task(context)

        # generate and dispatch reducer

    def make_mapper_context(self, taskid, input_fn):
        context = Context()
        context.jobid = self.id
        context.taskid = taskid
        context.mapper = self.mapper
        context.cnt_reducers = self.cnt_reducers
        context.input = input_fn
        return context

    def split_input(self):
        splitter = Splitter(5)
        results = []
        input_files = []
        for fname in self.inputs:
            input_files.append(RecordFile(fname, self.jobrunner.namenode))

        taskid = 0
        for block in splitter.split(input_files):
            fname = map_input(self.id, taskid)
            taskid += 1
            datanode = self.jobrunner.namenode.create_file(fname)
            for record in block:
                datanode.write_file(fname, record)
            datanode.close_file(fname)
            results.append(fname)

        return results
