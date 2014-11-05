import time
import logging
import Pyro4
from utils.conf import *

def retrieve_object(ns, name):
    uri = ns.lookup(name)
    return Pyro4.Proxy(uri)

def locateNS(**kwargs):
    ns = None
    for i in range(PYRO_LOCATE_NS_RETRY_TIMES):
        time.sleep(PYRO_LOCATE_NS_RETRY_INTERVAL)
        try:
            logging.info('Looking for Pyro name server at %s:%s' % \
                (kwargs['host'], kwargs['port']))
            ns = Pyro4.locateNS(kwargs['host'], int(kwargs['port']))
            logging.info("Ok! Pyro NS found.")
            break
        except Pyro4.errors.NamingError:
            logging.warning("Cannot locate Pyro NS, retry after %ds." % \
                PYRO_LOCATE_NS_RETRY_INTERVAL)

    return ns

def setup_Pyro_obj(obj, ns):
    host = obj.conf['host']
    port = int(obj.conf['port'])
    name = obj.conf['name']

    daemon = Pyro4.Daemon(host, port)
    uri = daemon.register(obj, name)
    ns.register(name, uri)

    return daemon
