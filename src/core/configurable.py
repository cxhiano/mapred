import Pyro4

class Configurable(object):
    def __init__(self, conf):
        for attr in conf:
            setattr(self, attr, conf[attr])

    def config_pyroNS(self):
        Pyro4.config.NS_HOST = self.pyroNS['host']
        Pyro4.config.NS_PORT = int(self.pyroNS['port'])
