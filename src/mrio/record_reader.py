class Record:
    def __init__(self, key, value):
        self.key = key
        self.value = value

class RecordReader:
    def __init__(self, record_len, filename, datanode):
        self.record_len = record_len
        self.filename = filename
        self.datanode = datanode

    def __iter__(self):
        offset = 0
        cnt = 0
        while True:
            cnt += 1
            rec = self.datanode.read_file(self.filename, offset, self.record_len)
            if len(rec) == 0:
                break
            yield Record(cnt, rec)
            offset += self.record_len
