""" Implementation of merge sort files in distributed file system """

import heapq, tempfile
import time
from utils.conf import *
from mrio.record_file import RecordFile

def _make_block(buf, tmpdir):
    file_ = tempfile.TemporaryFile(dir=tmpdir)
    buf.sort()
    for record in buf:
        file_.write(record)
    file_.seek(0)
    return file_

def sort_files(inputs, tmpdir, namenode):
    """ Perform merge sort on a list of files

    @param inputs A list of file names
    @param tmpdir Directory for temporary files created during the merge sort
    @param namnode The name node of the distributed file system
    @return The function yield record one by one after sorting
    """
    buf = []
    blocks = []
    for fname in inputs:
        file_ = RecordFile(fname, namenode)
        for record in file_:
            buf.append(record)
            if len(buf) == MAX_RECORDS_IN_BUFFER:
                blocks.append(_make_block(buf, tmpdir))
                buf = []

    if len(buf) > 0:
        blocks.append(_make_block(buf, tmpdir))

    for record in heapq.merge(*blocks):
        yield record
