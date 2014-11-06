import re

def map(key, value, out):
    for word in re.split('\\s+', value):
        if len(word) > 0:
            out.put(word, 1)

def reduce(key, values, out):
    cnt = 0
    for v in values:
        cnt += int(v)
    out.put(key, cnt)
