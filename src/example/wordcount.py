from src.lib.mapred import MapRed
import re

class WordCound(MapRed):
    def __init__(self):
        self.prev = None
        self.count = 0

    def map(self, key, value, out):
        for word in re.split('\\s+', value):
            if len(word) > 0:
                out.put(word, 1)

    def reduce(self, key, values, out):
        cnt = 0
        for v in values:
            cnt += int(v)
        out.put(key, cnt)
