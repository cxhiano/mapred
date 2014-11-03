import Pyro4

def retrieve_object(name):
    return Pyro4.Proxy(''.join(['PYRONAME:', name]))
