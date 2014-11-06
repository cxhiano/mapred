import copy

class Context:
    def validate(self):
        return True

    def partition(self, key, value):
        return str(key).__hash__() % self.cnt_reducers

    def clone(self):
        return copy.copy(self)
