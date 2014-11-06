class Splitter:
    def __init__(self, records_per_block):
        self.records_per_block = records_per_block

    def split(self, files):
        buf = []
        for f in files:
            for line in f:
                buf.append(line)
                if len(buf) == self.records_per_block:
                    pass
