class RecordFile:
    def __init__(self, filename, datanode):
        self.datanode = datanode
        self.filename = filename
        self.offset = 0

    def seek(self, offset):
        self.datanode.seek(self.filename, offset)
        self.offset = offset

    def close(self):
        self.datanode.close(self.filename)

    def read(self, nbytes):
        bytes_read = self.datanode.read_file(self.filename, nbytes)
        self.offset += bytes_read
        return bytes_read

    def write(self, buf):
        bytes_written = self.datanode.write_file(self.filename, buf)
        self.offset += bytes_written
        return bytes_written

    def readline(self):
        line = self.datanode.readline_file(self.filename)
        self.offset += len(line)
        return line

    def append(self, string):
        self.write(string)
        self.offset += len(string)

    def __iter__(self):
        while True:
            line = self.readline()
            if len(line) == 0:
                break
            yield line
