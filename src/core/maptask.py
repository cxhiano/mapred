from mrio.collector import OutputCollector
from mrio.record_file import RecordFile
from mrio.record_reader import RecordReader
from utils.filenames import *

class MapTask:
    def __init__(self, context):
        self.context = context.clone()
        self.context.out_files = []

    def create_output_files(self):
        datanode = None
        for i in range(self.context.cnt_reducers):
            fname = map_output(self.context.jobid, self.context.taskid, i)
            if datanode is None:
                datanode = self.context.namenode.create_file(fname)
            else:
                self.context.namenode.create_file(fname, preference=datanode)
            self.context.out_files.append(RecordFile(fname, self.context.namenode))

    def run(self):
        self.create_output_files()

        mapper = self.context.mapper
        input_fname = map_input(self.context.jobid, self.context.taskid)
        inp = RecordReader(RecordFile(input_fname, self.context.namenode))
        out = OutputCollector(self.context)

        for record in inp:
            mapper(record.key, record.value, out)

        out.flush()

        for file_ in self.context.out_files:
            file_.close()

    def cleanup(self):
        for file_ in self.context.out_files:
            file_.delete()
