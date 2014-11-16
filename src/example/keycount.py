"""Count python keywords"""
def map(key, value, out):
    from keyword import kwlist
    for word in value.split():
        if word in kwlist:
            out.put(word, 1)

def reduce(key, values, out):
    cnt = 0
    for v in values:
        cnt += int(v)
    out.put(key, cnt)
