from core.configurable import Configurable
from mrio.record_file import RecordFile
from mrio.record_reader import RecordReader
from utils.sortfiles import sort_files
from utils.filenames import *

class ReduceTask(Configurable):
    def __init__(self, task_conf, runner):
        self.load_dict(task_conf)
        self.runner = runner

    def run(self):
        namenode = self.runner.namenode

        input_fn = reduce_input(self.jobid, self.taskid)
        namenode.create_file(input_fn)
        input_ = RecordFile(input_fn, namenode)

        sort_files(self.inputs, '.', input_)



