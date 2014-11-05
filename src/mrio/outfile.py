from .conf import *

class MapperOutFile:
    @staticmethod
    def get_name(jobid, taskid, partitionid):
        return "%d_%d_%d" % (jobid, taskid, partitionid)
