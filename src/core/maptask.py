from core.configurable import Configurable
from mrio.collector import OutputCollector
from mrio.record_file import RecordFile
from utils.filenames import *

class MapTask(Configurable):
    def __init__(self, task_conf, runner):
        self.load_dict(task_conf)
        self.out_files = []
        self.runner = runner

    def create_output_files(self):
        datanode = None
        out_files = []
        namenode = self.runner.namenode

        for i in range(self.cnt_reducers):
            fname = map_output(self.jobid, self.taskid, i)
            if datanode is None:
                datanode = namenode.create_file(fname)
            else:
                namenode.create_file(fname, preference=datanode.get_name())

            out_files.append(RecordFile(fname, namenode))
        return out_files

    def run(self):
        out_files = self.create_output_files()

        input_ = RecordFile(self.input, self.runner.namenode)
        out = OutputCollector(out_files, self.cnt_reducers)

        line_num = 0
        for line in input_:
            self.mapper(line_num, line, out)
            line_num += 1

        out.flush()

        for file_ in out_files:
            file_.close()
