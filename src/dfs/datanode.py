import os
import logging
import threading
import Pyro4
from core.configurable import Configurable
from utils.conf_loader import load_config
from utils.sync import synchronized_method
from utils.rmi import *

def openfile(mode):
    def actual_decorator(func):
        def wrapper(self, filename, *args):
            if not filename in self.files:
                raise IOError('File not found')
            real_fn = self.real_filename(filename)

            file_ = self.files[filename]
            if file_ is None or file_.closed:
                file_ = open(real_fn, mode)
            elif not file_.mode in mode:
                raise IOError('Cannot read and write a file at the same time')

            self.files[filename] = file_

            return func(self, file_, *args)
        return wrapper
    return actual_decorator

class DataNode(Configurable):
    """ A data node in distributed file system """

    def __init__(self, conf):
        super(DataNode, self).__init__(load_config(conf))
        self.config_pyroNS()
        self.files = {}
        self.__lock__ = threading.RLock()

    def get_name(self):
        return self.name

    def run(self):
        self.ns = Pyro4.locateNS()

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.namenode)

        daemon = export(self)
        self.namenode.report(self.name)
        daemon.requestLoop()

    def real_filename(self, filename):
        return ''.join([self.datadir, filename])

    @synchronized_method('__lock__')
    def create_file(self, filename):
        real_fn = self.real_filename(filename)
        logging.debug('Creating file at %s' % real_fn)

        if filename in self.files:
            raise IOError('File %s already exists!' % filename)
        self.files[filename] = open(real_fn, 'w')

        self.namenode.create_file_meta(filename, self)

    @synchronized_method('__lock__')
    def delete_file(self, filename):
        if not filename in self.files:
            logging.warning('%s does not exist' % filename)
            return

        logging.info('deleting file %s' % filename)

        real_fn = self.real_filename(filename)
        os.remove(real_fn)
        del self.files[filename]
        self.namenode.delete_file_meta(filename)

    @synchronized_method('__lock__')
    @openfile('r')
    def read_file(self, file_, offset, nbytes):
        file_.seek(offset)
        return file_.read(nbytes)

    @synchronized_method('__lock__')
    @openfile('r')
    def readline_file(self, file_, offset):
        file_.seek(offset)
        return file_.readline()

    @synchronized_method('__lock__')
    @openfile('w')
    def write_file(self, file_, offset, buf):
        file_.seek(offset)
        file_.write(buf)
        file_.flush()
        return len(buf)

    @synchronized_method('__lock__')
    @openfile('rw')
    def seek_file(self, file_, offset):
        file_.seek(offset)

    @synchronized_method('__lock__')
    @openfile('rw')
    def close_file(self, file_):
        file_.close()

    def heart_beat(self):
        return True
