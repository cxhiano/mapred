
class MapTask:
    def __init__(self, taskid):
        self.id = taskid

    def run(self, MapperClass, inp, out):
        mapper = MapperClass()
        for record in inp:
            mapper.map(record.key, record.value, out)
