""" Implementation of merge sort """

import heapq, tempfile
import time
from utils.conf import *

def _record_iter(files):
    for f in files:
        for record in f:
            if len(record) > 0:
                yield record

def _make_block(buf, tmpdir):
    file_ = tempfile.TemporaryFile(dir=tmpdir)
    buf.sort()
    for record in buf:
        file_.write(record)
    file_.seek(0)
    return _record_iter([file_])

def sort_files(inputs, tmpdir):
    """ Perform merge sort on a list of files

    @param inputs A list of files
    @param tmpdir Directory for temporary files created during the merge sort
    @return The function yield record one by one after sorting
    """
    buf = []
    blocks = []
    for record in _record_iter(inputs):
        buf.append(record)
        if len(buf) == MAX_RECORDS_IN_BUFFER:
            blocks.append(_make_block(buf, tmpdir))
            buf = []
    if len(buf) > 0:
        blocks.append(_make_block(buf, tmpdir))

    for record in heapq.merge(*blocks):
        yield record
