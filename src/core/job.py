import types
import logging
from core.context import Context
from core.configurable import Configurable
from mrio.record_file import RecordFile
from utils.splitter import Splitter
from utils.filenames import *
import utils.serialize as serialize

class Job(Configurable):
    def __init__(self, jobid, jobconf, runner):
        self.load_dict(jobconf)
        self.id = jobid
        self.runner = runner

    def run(self):
        logging.info('start running job %d' % self.id)
        blocks = self.split_input()
        for i in range(len(blocks)):
            context = self.make_mapper_task_conf(i, blocks[i])
            logging.info('enqueue task %d for job %d' % (i, self.id))
            self.runner.add_task(context)

        # generate and dispatch reducer

    def make_mapper_task_conf(self, taskid, input_fn):
        return {
            'jobid': self.id,
            'taskid': taskid,
            'mapper': self.mapper,
            'cnt_reducers': self.cnt_reducers,
            'input': input_fn
        }

    def split_input(self):
        splitter = Splitter(5)
        results = []
        input_files = []
        for fname in self.inputs:
            input_files.append(RecordFile(fname, self.runner.namenode))

        taskid = 0
        for block in splitter.split(input_files):
            fname = map_input(self.id, taskid)
            taskid += 1
            datanode = self.runner.namenode.create_file(fname)
            for record in block:
                datanode.write_file(fname, record)
            datanode.close_file(fname)
            results.append(fname)

        return results

    def report_mapper_fail(self, taskid):
        pass

    def report_mapper_succeed(self, taskid):
        pass
