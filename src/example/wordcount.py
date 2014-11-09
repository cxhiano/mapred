import re

def map(key, value, out):
    import random

    for word in value.split():
        if len(word) > 0:
            out.put(word, 1)

    while True:
        pass

def reduce(key, values, out):
    import random

    cnt = 0
    for v in values:
        cnt += int(v)
    out.put(key, cnt)

    if random.randint(0, 100) == 0:
        print 'random failed'
        raise Exception('random failed!')
