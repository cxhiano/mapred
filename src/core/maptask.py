import sys
import logging
from core.task import Task
from mrio.collector import OutputCollector
from mrio.record_file import RecordFile
from utils.filenames import *

class MapTask(Task):
    def __init__(self, task_conf):
        super(MapTask, self).__init__(task_conf)
        self.out_files = []

    def create_output_files(self):
        datanode = None
        out_files = []

        for i in range(self.cnt_reducers):
            fname = map_output(self.jobid, self.taskid, i)
            if datanode is None:
                datanode = self.namenode.create_file(fname)
            else:
                datanode.create_file(fname)

            out_files.append(RecordFile(fname, self.namenode))
        return out_files

    def run(self):
        try:
            out_files = self.create_output_files()

            input_ = RecordFile(self.input, self.namenode)
            out = OutputCollector(out_files, self.cnt_reducers)

            line_num = 0
            for line in input_:
                self.mapper(line_num, line, out)
                line_num += 1

            out.flush()

            for file_ in out_files:
                file_.close()

        except:
            logging.info('map task %d for job %d failed: %s' % \
                (self.taskid, self.jobid, sys.exc_info()[1]))

            self.cleanup()

            self.jobrunner.report_mapper_fail(self.jobid, self.taskid)

            return

        logging.info('map task %d for job %d completed' % \
            (self.taskid, self.jobid))

        self.jobrunner.report_mapper_succeed(self.jobid, self.taskid)

    def cleanup(self):
        for i in range(self.cnt_reducers):
            fname = map_output(self.jobid, self.taskid, i)
            try:
                self.namenode.delete_file(fname)
            except IOError:
                logging.warning('Error deleting file %s in cleanup. Jobid: %d, \
                    Taskid: %d: %s' % (fname, self.jobid, self.taskid, \
                                       sys.exc_info()[1]))
    def kill(self):
        logging.info('map task %d for job %d failed: %s' % \
            (self.taskid, self.jobid, sys.exc_info()[1]))

        super(MapTask, self).kill(self)
