import types
from utils.serialize import *

class Job:
    def __init__(self, skeleton=None):
        if not skeleton is None:
            loads(self, skeleton)

    def validate(self):
        return True

    def serialize(self):
        return dumps(self)
