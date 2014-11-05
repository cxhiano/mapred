import copy

class Context:
    def __init__(self, jobid, cnt_mappers, cnt_reducers, Mapper, Reducer):
        self.jobid = jobid
        self.cnt_mappers =cnt_mappers
        self.cnt_reducers = cnt_reducers
        self.Mapper = Mapper
        self.Reducer = Reducer

    def partition(self, key, value):
        return str(key).__hash__() % self.cnt_reducers

    def clone(self):
        return copy.copy(self)
