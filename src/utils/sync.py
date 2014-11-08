import threading

def synchronized_func(func):
    func.__lock__ = threading.Lock()
    def sync_func(*args, **kwargs):
        with func.__lock__:
            return func(*args, **kwargs)
    return sync_func

def synchronized_method(attr):
    def sync_wrapper(method):
        def sync_method(self, *args, **kwargs):
            with getattr(self, attr):
                return method(self, *args, **kwargs)
        return sync_method
    return sync_wrapper

