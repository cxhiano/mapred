import types
import logging
import shutil
from core.conf import *
from core.configurable import Configurable
from core.tasklist import TaskList
from mrio.record_file import RecordFile
from utils.splitter import Splitter
from utils.filenames import *
import utils.serialize as serialize

MAP_PHASE = 0
REDUCE_PHASE = 1

class Job(Configurable):
    def __init__(self, jobid, jobconf, runner):
        super(Job, self).__init__(jobconf)
        self.id = jobid
        self.runner = runner

    def run(self):
        logging.info('start running job %d' % self.id)

        blocks = self.split_input()
        self.cnt_mappers = len(blocks)
        logging.info('Splitting input file done: %d blocks' % self.cnt_mappers)

        self.phase = MAP_PHASE
        self.list = TaskList(self.cnt_mappers)

        for taskid in self.list:
            task_conf = self.make_mapper_task_conf(taskid)
            self.runner.add_task(task_conf)
            logging.info('enqueued map task %d for job %d' % (taskid, self.id))

        self.phase = REDUCE_PHASE
        self.list = TaskList(self.cnt_reducers)

        for taskid in self.list:
            task_conf = self.make_reducer_task_conf(taskid)
            self.runner.add_task(task_conf)
            logging.info('enqueued reduce task %d for job %d' % (taskid, self.id))

        self.cleanup()
        self.runner.report_job_succeed(self.id)

    def make_mapper_task_conf(self, taskid):
        return {
            'jobid': self.id,
            'taskid': taskid,
            'mapper': self.mapper,
            'cnt_reducers': self.cnt_reducers,
            'input': map_input(self.id, taskid)
        }

    def make_reducer_task_conf(self, taskid):
        return {
            'jobid': self.id,
            'taskid': taskid,
            'reducer': self.reducer,
            'output_dir': self.output_dir,
            'inputs': [map_output(self.id, i, taskid) for i in \
                range(self.cnt_mappers)]
        }

    def split_input(self):
        splitter = Splitter(RECORDS_PER_BLOCK)
        results = []
        input_files = []
        for fname in self.inputs:
            input_files.append(RecordFile(fname, self.runner.namenode))

        taskid = 0
        datanode = None
        for block in splitter.split(input_files):
            fname = map_input(self.id, taskid)
            taskid += 1
            if datanode is None:
                datanode = self.runner.namenode.create_file(fname)
            else:
                datanode.create_file(fname)
            for record in block:
                datanode.write_file(fname, 0, record)
            datanode.close_file(fname)
            results.append(fname)

        for file_ in input_files:
            file_.close()

        return results

    def report_mapper_fail(self, taskid):
        self.list.report_failed(taskid)

    def report_mapper_succeed(self, taskid):
        self.list.report_succeeded(taskid)

    def report_reducer_fail(self, taskid):
        self.list.report_failed(taskid)

    def report_reducer_succeed(self, taskid):
        self.list.report_succeeded(taskid)

    def cleanup(self):
        namenode = self.runner.namenode
        for i in range(self.cnt_mappers):
            fname = map_input(self.id, i)

            try:
                namenode.delete_file(fname)
            except IOError:
                logging.warning('Error deleting file %s' % fname)

            for j in range(self.cnt_reducers):
                fname = map_output(self.id, i, j)

                try:
                    namenode.delete_file(fname)
                except IOError:
                    logging.warning('Error deleting file %s' % fname)
