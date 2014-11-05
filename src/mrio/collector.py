from .conf import *

class OutputCollector:
    def __init__(self, file_):
        self.file_ = file_
        self.buffer = []

    def put(self, key, value):
        print key, value
        self.buffer.append((key, value))
        if len(self.buffer) >= SPILL_THESHOLD:
            self.spill()

    def spill(self):
        for key, value in self.buffer:
            self.file_.write('%s\t%s\n', (key, value))
        self.buffer = []
