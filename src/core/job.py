import types
import utils.serialize as serialize

class Job:
    def __init__(self, jobconf):
        serialize.loads(self, jobconf)
