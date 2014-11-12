""" This module provides ways to access a file in distributed system and
interpret its content as records. (key/value pair)

In a record file, \n is interpreted as separator of records and \t is
interpreted as separator of key and value
"""

def record_iter(file_):
    """ Iter through a record file and yield key value pair for each record
    in the file
    """
    for record in file_:
        yield record[:-1].split('\t')

class RecordFile:
    """ A file in distributed system whose content can be interpreted as records

    This object is NOT thread safe.
    """
    def __init__(self, filename, namenode):
        self.filename = filename
        self.namenode = namenode
        self.offset = 0

    def seek(self, offset):
        self.offset = offset

    def close(self):
        self.namenode.close_file(self.filename)

    def read(self, nbytes):
        bytes_read = self.namenode.read_file(self.filename, self.offset, nbytes)
        self.offset += bytes_read
        return bytes_read

    def write(self, buf):
        bytes_written = self.namenode.write_file(self.filename, self.offset,
            buf)
        self.offset += bytes_written
        return bytes_written

    def readline(self):
        line = self.namenode.readline_file(self.filename, self.offset)
        self.offset += len(line)
        return line

    def __iter__(self):
        while True:
            line = self.readline()
            if len(line) == 0:
                break
            yield line
