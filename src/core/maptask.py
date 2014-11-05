from mrio.outfile import MapperOutFile
from mrio.collector import OutputCollector

class MapTask:
    def __init__(self, taskid, context):
        self.id = taskid
        self.context = context.clone()
        self.context.taskid = taskid
        self.context.out_files = []

    def create_output_files(self):
        datanode = None
        for fname in MapperOutFile.get_all_names(self.context.jobid,
                self.context.taskid):
            if datanode is None:
                datanode = self.context.name_node.create_file(fname)
            else:
                self.context.name_node.create_file(fname, preference=datanode)
            self.context.out_files.append(RecordFile(fname, datanode))

    def run(self, MapperClass, inp):
        mapper = MapperClass()
        out = OutputCollector(self.context)

        for record in inp:
            mapper.map(record.key, record.value, out)

        for file_ in self.out_files:
            file_.close()
