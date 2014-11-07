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

        for record in record_iter(reduce_input):
            print record
            # self.reduce(key, value, out)
