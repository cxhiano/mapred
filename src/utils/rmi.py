""" This module provides helper functions to retrieve and export remote objects
using Pyro4
"""
import time
import logging
import Pyro4

def retrieve_object(ns, name):
    uri = ns.lookup(name)
    return Pyro4.Proxy(uri)

def export(obj):
    daemon = Pyro4.Daemon(obj.host, int(obj.port))
    uri = daemon.register(obj, obj.name)
    obj.ns.register(obj.name, uri)

    return daemon
