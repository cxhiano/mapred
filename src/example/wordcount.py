import re

def map(key, value, out):
    import random

    for word in value.split():
        if len(word) > 0:
            out.put(word, 1)

    if random.randint(0, 8) == 0:
        print 'random failed'
        while True:
            pass

def reduce(key, values, out):
    import random

    cnt = 0
    for v in values:
        cnt += int(v)
    out.put(key, cnt)

    if random.randint(0, 50) == 0:
        print 'random failed'
        raise Exception('random failed!')
