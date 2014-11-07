class Configurable:
    def load_dict(self, dic):
        for attr in dic:
            setattr(self, attr, dic[attr])
