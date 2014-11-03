
class MapTask:
    def __init__(self):
        pass

    def run(self, MapperClass, inp, out):
        mapper = MapperClass()
        for record in inp:
            mapper.map(record.key, record.value, out)
