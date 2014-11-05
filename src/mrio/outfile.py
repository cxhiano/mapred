from .conf import *

class MapperOutFile:
    @staticmethod
    def get_name(jobid, taskid, partitionid):
        return "%d_%d_%d" % (jobid, taskid, partitionid)

    @staticmethod
    def get_all_names(jobid, taskid, num_reducer):
        for i in range(num_reducer):
            yield MapperOutFile.get_name(jobid, taskid, i)
