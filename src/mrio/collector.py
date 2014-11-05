from .conf import *
from mrio.outfile import MapperOutFile

class OutputCollector:
    def __init__(self, context):
        self.context = context
        self.buffer = []

    def put(self, key, value):
        print key, value
        self.buffer.append((key, value))
        if len(self.buffer) >= SPILL_THESHOLD:
            self.flush()

    def flush(self):
        file_ = self.context.out_files[self.context.partition(key, value)]

        for key, value in self.buffer:
            file_.write('%s\t%s\n', (key, value))
        self.buffer = []
