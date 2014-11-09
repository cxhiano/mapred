import os
from core.task import Task
from mrio.record_file import RecordFile
from mrio.record_reader import record_iter
from mrio.collector import OutputCollector
from utils.sortfiles import sort_files
from utils.filenames import *

class ReduceTask(Task):
    def __init__(self, task_conf):
        super(ReduceTask, self).__init__(task_conf)

    def run(self):
        inputs = [RecordFile(fname, self.runner.namenode) for fname in \
            self.inputs]
        reduce_input = sort_files(inputs, self.tmpdir)
        output_file = RecordFile(self.output_fname, self.runner.namenode)
        out = OutputCollector([output_file], 1)

        prev_key = None
        values = []
        for key, value in record_iter(reduce_input):
            if prev_key is None or key == prev_key:
                values.append(value)
            else:
                self.reducer(prev_key, values, out)
                values = []
            prev_key = key

        if len(values) > 0:
            self.reducer(prev_key, values, out)

        out.flush()
        output_file.close()

    def cleanup(self):
        try:
            self.runner.namenode.delete_file(self.output_fname)
        except IOError:
            logging.warning('Error deleting file %s in cleanup. Jobid: %d, \
                Taskid: %d: %s' % (self.output_fname, self.jobid, self.taskid, \
                                   sys.exc_info()[1]))
