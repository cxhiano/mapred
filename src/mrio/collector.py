from .conf import *
from mrio.outfile import MapperOutFile

class OutputCollector:
    def __init__(self, out_files, partitions):
        self.out_files = out_files
        self.partitions = partitions
        self.buffer = []

    def partition(self, key):
        return str(key).__hash__() % self.partitions

    def put(self, key, value):
        print key, value
        self.buffer.append((key, value))
        if len(self.buffer) >= SPILL_THESHOLD:
            self.flush()

    def flush(self):
        for key, value in self.buffer:
            file_ = self.out_files[self.partition(key)]
            file_.write('%s\t%s\n' % (key, value))
        self.buffer = []
