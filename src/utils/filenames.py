""" This module specifies file names for map reduce tasks

Files created during mapreduce is named by combining its usage, job id and task
id so that they are globally unique among jobs and tasks
"""

def map_input(jobid, taskid):
    return 'mapin_%d_%d' % (jobid, taskid)

def map_output(jobid, taskid, partitionid):
    return 'mapout_%d_%d_%d' % (jobid, taskid, partitionid)

def reduce_input(jobid, taskid):
    return 'reducein_%d_%d' % (jobid, taskid)

def reduce_output(jobid, taskid):
    return 'part_%d' % taskid
