import copy

class Context:
    def __init__(self, skeleton=None):
        if not skeleton is None:
            for attr in skeleton:
                value = skeleton[attr]
                if isinstance(value, types.CodeType):
                    setattr(self, attr, types.FunctionType(value, {}))
                else:
                    setattr(self, attr, skeleton[attr])

    def validate(self):
        return True

    def partition(self, key, value):
        return str(key).__hash__() % self.cnt_reducers

    def clone(self):
        return copy.copy(self)

    def serialize(self):
        skeleton = {}
        for attr_name in dir(self):
            if attr_name.startswith('_'):
                continue
            attr = getattr(self, attr_name)
            if isinstance(attr, types.MethodType):
                continue
            if isinstance(attr, types.FunctionType):
                skeleton[attr_name] = attr.func_code
            else:
                skeleton[attr_name] = attr

        return skeleton
