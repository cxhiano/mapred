import time
import Pyro4
from core.maptask import MapTask
from core.reducetask import ReduceTask
from core.jobrunner import JobRunner
from utils.rmi import *
import utils.serialize as serialize
import example.wordcount as wordcount

ns = Pyro4.locateNS(port=9999)
namenode = retrieve_object(ns, 'NameNode')

def create_input(fname, namenode):
    content = '''
              Logging is a means of tracking events that happen when some software runs.
              The software's developer adds logging calls to their code to indicate that
              certain events have occurred. An event is described by a descriptive message
              which can optionally contain variable data (i.e. data that is potentially
              different for each occurrence of the event). Events also have an importance
              which the developer ascribes to the event; the importance can also be called
              the level or severity.
              '''

    datanode = namenode.create_file(fname)
    datanode.write_file(fname, 0, content)
    datanode.close_file(fname)

if __name__ == '__main__':
    create_input('a.txt', namenode)
    create_input('b.txt', namenode)
    create_input('c.txt', namenode)
    create_input('d.txt', namenode)

    Pyro4.config.SERIALIZER = 'marshal'
    jobconf1 = {
        'mapper': wordcount.map,
        'reducer': wordcount.reduce,
        'cnt_reducers': 2,
        'inputs': ['a.txt', 'b.txt'],
        'output_dir': 'mytask0'
    }

    jobconf2 = {
        'mapper': wordcount.map,
        'reducer': wordcount.reduce,
        'cnt_reducers': 1,
        'inputs': ['a.txt', 'b.txt'],
        'output_dir': 'mytask1'
    }

    jr = retrieve_object(ns, 'JobRunner')
    jr.submit_job(serialize.dumps(jobconf1))
    # jr.submit_job(serialize.dumps(jobconf2))
