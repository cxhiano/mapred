class Splitter:
    """ Split record files """

    def __init__(self, records_per_block):
        self.records_per_block = records_per_block

    def split(self, files):
        """ Split files to blocks

        Each of the files produced by splitter has at most records_per_block
        records.
        """
        buf = []
        for f in files:
            for line in f:
                buf.append(line)
                if len(buf) == self.records_per_block:
                    yield buf
                    buf = []

        if len(buf) != 0:
            yield buf
