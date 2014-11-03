class Record:
    def __init__(self, key, value):
        self.key = key
        self.value = value

class RecordReader:
    def __init__(self, file_):
        self.file_ = file_

    def __iter__(self):
        cnt = 0
        for line in self.file_:
            cnt += 1
            yield Record(cnt, line)
