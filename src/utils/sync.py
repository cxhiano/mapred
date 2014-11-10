""" This module provides decorator for synchronization """

import threading

def synchronized_func(func):
    """ Decorator for a synchronized function.

    An exclusive lock will be acquired before running the function and released
    finally
    """
    func.__lock__ = threading.Lock()
    def sync_func(*args, **kwargs):
        with func.__lock__:
            return func(*args, **kwargs)
    return sync_func

def synchronized_method(attr):
    """ Decorator for a synchronized method

    @param attr The attribute of the object which will be used as lock to
    synchronize to the method.
    """
    def sync_wrapper(method):
        def sync_method(self, *args, **kwargs):
            with getattr(self, attr):
                return method(self, *args, **kwargs)
        return sync_method
    return sync_wrapper

