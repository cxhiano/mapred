import re

def map(key, value, out):
    import random

    for word in value.split():
        if len(word) > 0:
            out.put(word, 1)

    if random.randint(0, 5) == 0:
        raise Exception('random failed!')

def reduce(key, values, out):
    import random

    cnt = 0
    for v in values:
        cnt += int(v)
    out.put(key, cnt)

    if random.randint(0, 10) == 0:
        raise Exception('random failed!')
