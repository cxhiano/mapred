import copy
from utils.serialize import *

class Context:
    def __init__(self, skeleton=None):
        if not skeleton is None:
            loads(self, skeleton)

    def validate(self):
        return True

    def partition(self, key, value):
        return str(key).__hash__() % self.cnt_reducers

    def clone(self):
        return copy.copy(self)

    def serialize(self):
        return dumps(self)
