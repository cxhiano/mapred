import time
import Pyro4
from conf import *

def retrieve_object(name):
    return Pyro4.Proxy(''.join(['PYRONAME:', name]))

def locateNS(**kwargs):
    ns = None
    for i in range(PYRO_LOCATE_NS_RETRY_TIMES):
        try:
            ns = Pyro4.locateNS(kwargs['host'], int(kwargs['port']))
            print "Ok! Pyro NS found."
            break
        except Pyro4.errors.NamingError:
            print "Cannot locate Pyro NS, retry after %ds." % \
                PYRO_LOCATE_NS_RETRY_INTERVAL
            time.sleep(PYRO_LOCATE_NS_RETRY_INTERVAL)

    return ns

def setup_Pyro_obj(obj, name, host, port, ns):
    daemon = Pyro4.Daemon(host, port)
    uri = daemon.register(obj, name)
    ns.register(name, uri)

    return daemon
