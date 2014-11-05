import Pyro4
from utils.conf import load_config
import os

def openfile(func):
    def wrapper(self, filename, *args):
        file_ = self.files.get(self.real_filename(filename))
        if file_ is None:
            raise IOError('File not found')
        return func(self, file_, *args)
    return wrapper

class DataNode:
    """ A data node in distributed file system """

    def __init__(self, conf):
        self.conf = load_config(conf)
        self.files = {}

    def run(self):
        Pyro4.Daemon.serveSimple(
            {
                self: self.conf['name']
            },
            port=int(self.conf['port']),
            ns=True)

    def real_filename(self, filename):
        return filename

    def create_file(self, filename):
        real_fn = self.abs_filename(filename)
        f = self.files.get(real_fn)
        if f is None:
            self.files[filename] = open(filename_fn, 'rw')
        else:
            raise IOError('File already exists!')

    @openfile
    def delete_file(self, file_):
        os.remove(file_.name)
        del self.files[file_.name]

    @openfile
    def read_file(self, file_, nbytes):
        return file_.read(nbytes)

    @openfile
    def readline_file(self, file_):
        return file_.readline()

    @openfile
    def write_file(self, file_, buf):
        return file_.write(buf)

    @openfile
    def seek(self, file_, offset):
        file_.seek(offset)

    def heart_beat(self):
        return True
