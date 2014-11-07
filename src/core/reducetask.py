import os
from core.configurable import Configurable
from mrio.record_file import RecordFile
from mrio.record_reader import record_iter
from mrio.collector import OutputCollector
from utils.sortfiles import sort_files
from utils.filenames import *

class ReduceTask(Configurable):
    def __init__(self, task_conf, runner):
        self.load_dict(task_conf)
        self.runner = runner

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
