class RecordFile:
    def __init__(self, filename, namenode):
        self.datanode = namenode.get_file(filename)
        self.filename = filename
        self.offset = 0

    def seek(self, offset):
        self.offset = offset

    def read(self, nbytes):
        return self.datanode.read_file(self.filename, self.offset, nbytes)

    def write(self, buf):
        self.datanode.write_file(self.filename, buf, self.offset, len(buf))

    def readline(self):
        return self.datanode.readline_file(self.filename, self.offset)

    def __iter__(self):
        while True:
            tmp = self.readline()
            if len(tmp) == 0:
                break
            yield tmp
            self.offset += len(tmp)
