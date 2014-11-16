"""A slower word count program, sleep for one second after each call of map/reduce"""
def map(key, value, out):
    import time

    for word in value.split():
        if len(word) > 0:
            out.put(word, 1)

    time.sleep(1)

def reduce(key, values, out):
    import time

    cnt = 0
    for v in values:
        cnt += int(v)
    out.put(key, cnt)
    time.sleep(1)
