import time
import logging
import Pyro4
from utils.conf import *

def retrieve_object(ns, name):
    uri = ns.lookup(name)
    return Pyro4.Proxy(uri)

def locateNS(host, port):
    ns = None
    for i in range(PYRO_LOCATE_NS_RETRY_TIMES):
        time.sleep(PYRO_LOCATE_NS_RETRY_INTERVAL)
        try:
            logging.info('Looking for Pyro name server at %s:%s' % \
                (host, port))
            ns = Pyro4.locateNS(host, port)
            logging.info("Ok! Pyro NS found.")
            break
        except Pyro4.errors.NamingError:
            logging.warning("Cannot locate Pyro NS, retry after %ds." % \
                PYRO_LOCATE_NS_RETRY_INTERVAL)

    return ns

def export(obj):
    daemon = Pyro4.Daemon(obj.host, int(obj.port))
    uri = daemon.register(obj, obj.name)
    obj.ns.register(obj.name, uri)

    return daemon
