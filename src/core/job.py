import types
import logging
import shutil
from core.conf import *
from core.configurable import *
from core.tasklist import TaskList
from mrio.record_file import RecordFile
from utils.splitter import Splitter
from utils.filenames import *
import utils.serialize as serialize

MAP_PHASE = 0
REDUCE_PHASE = 1

class Job(Configurable):
    """ A job in map reduce system """

    def __init__(self, jobid, jobconf, runner):
        super(Job, self).__init__(jobconf)
        self.runner = runner
        self.id = jobid
        self.open_files = []
        self.result_files = []
        self.terminate_flag = False

        self.finished_mappers = 0
        self.failed_mappers = 0
        self.finished_reducers = 0
        self.failed_reducers = 0

    def terminate(self):
        """ Terminate the job """
        self.terminate_flag = True
        self.list.report_failed(0)

    def validate(self, jobconf):
        """ Validate job config """
        cnt_reducers = jobconf.get('cnt_reducers')
        if type(cnt_reducers) != int or cnt_reducers <= 0:
            raise ValidationError('Incorrect cnt_reducers')

        output_dir = jobconf.get('output_dir')
        if type(output_dir) != str:
            raise ValidationError('output_dir must be a string')

        inputs = jobconf.get('inputs')
        if type(inputs) != list:
            raise ValidationError('inputs must be list of file name')
        for fname in inputs:
            if type(fname) != str or len(fname) == 0:
                raise ValidationError('inputs must be list of file name')

        mapper = jobconf.get('mapper')
        if not isinstance(mapper, types.FunctionType) or \
                mapper.func_code.co_argcount != 3:
            raise ValidationError('Invalid mapper')

        reducer = jobconf.get('reducer')
        if not isinstance(reducer, types.FunctionType) or \
                mapper.func_code.co_argcount != 3:
            raise ValidationError('Invalid reducer')

    def run(self):
        """ Run the job

        1. Split input files into blocks
        2. Create all needed file in map reduce process
        3. Generate and send map task to job runner for dispatching
        4. Generate and send reduce task to job runner for dispatching
        5. Clean up
        """
        logging.info('start running job %d' % self.id)

        try:
            blocks = self.split_input()
        except Exception as e:
            logging.info('job %d split input error: %s' % (self.id, e.message))
            self.fail()
            return
        self.cnt_mappers = len(blocks)
        logging.info('Splitting input file done: %d blocks' % self.cnt_mappers)

        try:
            self.create_output_files()
        except Exception as e:
            logging.info('job %d create output files error: %s' % (self.id,
                e.message))
            self.fail()
            return
        logging.info('job %d: create input files done' % self.id)

        self.phase = MAP_PHASE
        self.list = TaskList(self.cnt_mappers)

        while True:
            if self.list.fails >= JOB_MAXIMUM_TASK_FAILURE or \
                    self.terminate_flag:
                logging.info('job %d terminated: %d tasks failed' % (self.id,
                    self.list.fails))
                self.fail()
                return
            try:
                taskid = self.list.next(JOB_RUNNER_TIMEOUT)
            except:
                logging.info('job %d: map timeout! Kill all tasks' % self.id)
                self.runner.kill_all_tasks(self)
                continue
            if taskid is None:
                break
            task_conf = self.make_mapper_task_conf(taskid)
            self.runner.add_task(task_conf)
            logging.info('enqueued map task %d for job %d' % (taskid, self.id))

        self.phase = REDUCE_PHASE
        self.list = TaskList(self.cnt_reducers)

        while True:
            if self.list.fails >= JOB_MAXIMUM_TASK_FAILURE or \
                    self.terminate_flag:
                logging.info('job %d terminated: %d tasks failed' % (self.id,
                    self.list.fails))
                self.fail()
                return
            try:
                taskid = self.list.next(JOB_RUNNER_TIMEOUT)
            except:
                logging.info('job %d: reduce timeout! Kill all tasks' % self.id)
                self.runner.kill_all_tasks(self)
                continue
            if taskid is None:
                break
            task_conf = self.make_reducer_task_conf(taskid)
            self.runner.add_task(task_conf)
            logging.info('enqueued reduce task %d for job %d' % (taskid, self.id))

        for fname in self.result_files:
            self.open_files.remove(fname)
        self.cleanup()
        self.runner.report_job_succeed(self.id)

    def fail(self):
        """ The job failed, do clean up and report to job runner """
        self.cleanup()
        self.runner.report_job_fail(self.id)

    def make_mapper_task_conf(self, taskid):
        """ Generate map task config for given taskid """
        return {
            'jobid': self.id,
            'taskid': taskid,
            'mapper': self.mapper,
            'cnt_reducers': self.cnt_reducers,
            'input': map_input(self.id, taskid)
        }

    def make_reducer_task_conf(self, taskid):
        """ Generate reduce task config for given taskid """
        return {
            'jobid': self.id,
            'taskid': taskid,
            'reducer': self.reducer,
            'output_dir': self.output_dir,
            'inputs': [map_output(self.id, i, taskid) for i in \
                range(self.cnt_mappers)]
        }

    def split_input(self):
        """ Split input files into blocks """
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

            bytes_written = 0
            for record in block:
                bytes_written += datanode.write_file(fname, bytes_written,
                    record)

            datanode.close_file(fname)
            results.append(fname)
            self.open_files.append(fname)

        for file_ in input_files:
            file_.close()

        return results

    def create_output_files(self):
        """ Create output files that will be used during the job

        All reducer output files and mapper output files will be created
        """
        datanode = None
        for i in range(self.cnt_reducers):
            fname = '%s.%s' % (self.output_dir, reduce_output(self.id, i))
            if datanode is None:
                datanode = self.runner.namenode.create_file(fname)
            else:
                datanode.create_file(fname)
            self.result_files.append(fname)
            self.open_files.append(fname)

            for j in range(self.cnt_mappers):
                fname = map_output(self.id, j, i)
                datanode.create_file(fname)
                self.open_files.append(fname)

    def report_task_fail(self, taskid):
        if self.phase == MAP_PHASE:
            self.failed_mappers += 1
        else:
            self.failed_reducers += 1
        self.list.report_failed(taskid)

    def report_task_succeed(self, taskid):
        if self.phase == MAP_PHASE:
            self.finished_mappers += 1
        else:
            self.finished_reducers += 1
        self.list.report_succeeded(taskid)

    def cleanup(self):
        namenode = self.runner.namenode
        for fname in self.open_files:
            try:
                namenode.delete_file(fname)
            except Exception as e:
                logging.warning('Error deleting file %s: %s' % (fname, e.message))
