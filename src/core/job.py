import marshal
import types

class Job:
    def __init__(self, skeleton=None):
        if not skeleton is None:
            for attr in skeleton:
                setattr(self, attr, skeleton[attr])
            self.mapper = types.FunctionType(self.mapper, {})
            self.reducer = types.FunctionType(self.reducer, {})

    def validate(self):
        return True

    def serialize(self):
        skeleton = {
            'mapper': self.mapper.func_code,
            'reducer': self.mapper.func_code,
            'cnt_reducers': self.cnt_reducers,
            'inputs': self.inputs,
            'output_dir': self.output_dir
            }
        return skeleton
