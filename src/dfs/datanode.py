import os
import logging
import Pyro4
from utils.conf_loader import load_config
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

class DataNode:
    """ A data node in distributed file system """

    def __init__(self, conf_file):
        self.conf = load_config(conf_file)
        self.files = {}

    def get_conf(self, key):
        return self.conf[key]

    def run(self):
        self.ns = locateNS(**self.conf['pyroNS'])
        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        daemon = setup_Pyro_obj(self, self.ns)

        namenode = retrieve_object(self.ns, self.conf['namenode'])
        namenode.report(self.conf['name'])

        daemon.requestLoop()

    def real_filename(self, filename):
        return ''.join([self.conf['datadir'], filename])

    def create_file(self, filename):
        real_fn = self.real_filename(filename)
        logging.debug('Creating file at %s' % real_fn)

        if filename in self.files:
            raise IOError('File already exists!')
        self.files[filename] = None

    @openfile('rw')
    def delete_file(self, file_):
        self.close(file_)
        os.remove(file_.name)
        del self.files[file_.name]

    @openfile('r')
    def read_file(self, file_, nbytes):
        return file_.read(nbytes)

    @openfile('r')
    def readline_file(self, file_):
        return file_.readline()

    @openfile('w')
    def write_file(self, file_, buf):
        file_.write(buf)
        return len(buf)

    @openfile('rw')
    def seek_file(self, file_, offset):
        file_.seek(offset)

    @openfile('rw')
    def close_file(self, file_):
        file_.close()

    def heart_beat(self):
        return True
