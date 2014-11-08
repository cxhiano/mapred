import time
import Pyro4
from core.maptask import MapTask
from core.reducetask import ReduceTask
from core.context import Context
from core.jobrunner import JobRunner
from utils.rmi import *
import utils.serialize as serialize
import example.wordcount as wordcount

ns = Pyro4.locateNS(port=8888)
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
    datanode.write_file(fname, content)
    datanode.close_file(fname)

def test_map():
    create_input('a.txt', namenode)
    task_conf = {
        'jobid': 1,
        'taskid': 1,
        'mapper': wordcount.map,
        'cnt_reducers': 2,
        'input': 'a.txt'
    }
    c = Context()
    c.namenode = namenode

    task = MapTask(task_conf, c)
    task.run()

if __name__ == '__main__':
    create_input('a.txt', namenode)
    create_input('b.txt', namenode)
    create_input('c.txt', namenode)
    create_input('d.txt', namenode)

    Pyro4.config.SERIALIZER = 'marshal'
    jobconf1 = {
        'mapper': wordcount.map,
        'reducer': wordcount.reduce,
        'cnt_reducers': 1,
        'inputs': ['a.txt', 'b.txt'],
        'output_dir': 'mytask1'
    }

    jobconf2 = {
        'mapper': wordcount.map,
        'reducer': wordcount.reduce,
        'cnt_reducers': 1,
        'inputs': ['c.txt', 'd.txt'],
        'output_dir': 'mytask2'
    }

    jr = retrieve_object(ns, 'JobRunner')
    jr.submit_job(serialize.dumps(jobconf1))
    jr.submit_job(serialize.dumps(jobconf2))
