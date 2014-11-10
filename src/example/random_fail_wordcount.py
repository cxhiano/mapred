import random

def map(key, value, out):
    for word in value.split():
        if len(word) > 0:
            out.put(word, 1)
    if random.randint(0, 5) == 0:
        print 'random failed'
        while True:
            pass

def reduce(key, values, out):
    cnt = 0
    for v in values:
        cnt += int(v)
    out.put(key, cnt)
    if random.randint(0, 5) == 0:
        print 'random failed'
        while True:
            pass
