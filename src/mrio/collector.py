class OutputCollector:
    def __init__(self, file_):
        self.file_ = file_

    def put(self, key, value):
        print key, value

