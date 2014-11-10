from mrio.conf import *
from mrio.outfile import MapperOutFile

class OutputCollector:
    """ Collect output from map/reduce

    Output from map/reduce will be stored in a buffer until the number of
    records in buffer reach SPILL_THESHOLD. Then the content of buffer will be
    flushed to file.
    """

    def __init__(self, out_files):
        """ out_files is list of output files. A record will be dispatched to
        one of these file according to the hash code of its key.
        """
        self.out_files = out_files
        self.buffer = []

    def partition(self, key):
        """ Determine the partition of the record by its key """
        return str(key).__hash__() % len(self.out_files)

    def put(self, key, value):
        """ Method for map/reduce output """
        self.buffer.append((key, value))
        if len(self.buffer) >= SPILL_THESHOLD:
            self.flush()

    def flush(self):
        """ Flush buffer to file """
        for key, value in self.buffer:
            file_ = self.out_files[self.partition(key)]
            file_.write('%s\t%s\n' % (key, value))
        self.buffer = []
