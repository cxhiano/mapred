import Pyro4

class ValidationError(Exception):
    def __init__(self, msg):
        self.msg = msg

class Configurable(object):
    """ An remote object that can be configured by a dict

    Keys in dict will be set as attributes to the object
    """
    def __init__(self, conf):
        self.validate(conf)
        for attr in conf:
            setattr(self, attr, conf[attr])

    def config_pyroNS(self):
        Pyro4.config.NS_HOST = self.pyroNS['host']
        Pyro4.config.NS_PORT = int(self.pyroNS['port'])

    def validate(self, conf):
        pass
