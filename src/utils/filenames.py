def map_input(jobid, taskid):
    return 'mapin_%d_%d' % (jobid, taskid)

def map_output(jobid, taskid, partitionid):
    return 'mapout_%d_%d_%d' % (jobid, taskid, partitionid)

def reduce_input(jobid, taskid):
    return 'reducein_%d_%d' % (jobid, taskid)
