import heapq, tempfile
import time
from utils.conf import *

def line_iter(files):
    for f in files:
        for line in f:
            if len(line) > 0:
                yield line

def make_block(buf, tmpdir):
    file_ = tempfile.TemporaryFile(dir=tmpdir)
    buf.sort()
    for line in buf:
        file_.write(line)
    file_.seek(0)
    return line_iter([file_])

def sort_files(inputs, tmpdir):
    buf = []
    blocks = []
    for line in line_iter(inputs):
        buf.append(line)
        if len(buf) == MAX_LINE_IN_BUFFER:
            blocks.append(make_block(buf, tmpdir))
            buf = []
    if len(buf) > 0:
        blocks.append(make_block(buf, tmpdir))

    for line in heapq.merge(*blocks):
        yield line
