from mrio.outfile import MapperOutFile
from mrio.collector import OutputCollector
from mrio.record_file import RecordFile
from mrio.record_reader import RecordReader
from utils.filenames import *

class MapTask:
    def __init__(self, taskid, context):
        self.id = taskid
        self.context = context.clone()
        self.context.taskid = taskid
        self.context.out_files = []

    def create_output_files(self):
        datanode = None
        for fname in MapperOutFile.get_all_names(self.context.jobid,
                self.context.taskid, self.context.cnt_reducers):
            if datanode is None:
                datanode = self.context.namenode.create_file(fname)
            else:
                self.context.namenode.create_file(fname, preference=datanode)
            self.context.out_files.append(RecordFile(fname, self.context.namenode))

    def run(self):
        self.create_output_files()

        mapper = self.context.Mapper()
        input_fname = map_input(self.context.jobid, self.context.taskid)
        inp = RecordReader(RecordFile(input_fname, self.context.namenode))
        out = OutputCollector(self.context)

        for record in inp:
            mapper.map(record.key, record.value, out)

        out.flush()

        for file_ in self.context.out_files:
            file_.close()

    def cleanup(self):
        for file_ in self.context.out_files:
            file_.delete()
