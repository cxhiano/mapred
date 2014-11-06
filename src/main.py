from mrio.collector import OutputCollector
from mrio.record_reader import RecordReader
from mrio.record_file import RecordFile
from core.maptask import MapTask
from core.context import Context
from utils.rmi import *
from core.jobrunner import JobRunner
from core.taskrunner import TaskRunner
from core.job import Job
import example.wordcount as wordcount
import Pyro4

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
    context = Context(2, 10, 2, WordCount, WordCount)
    context.namenode =namenode
    context.input = 'a.txt'
    task = MapTask(2, context)
    task.run()

if __name__ == '__main__':
  create_input('a.txt', namenode)
  create_input('b.txt', namenode)
  Pyro4.config.SERIALIZER = 'marshal'
  tr = TaskRunner('conf/task_runner.xml')
  jr = retrieve_object(ns, 'JobRunner')
  job = Job()
  job.mapper = wordcount.map
  job.reducer = wordcount.reduce
  job.cnt_reducers = 2
  job.inputs = ['a.txt', 'b.txt']
  job.output_dir = '.'
  print jr.submit_job(job.serialize())
  context = Context(jr.get_task())
  context.namenode = namenode
  maptask = MapTask(context)
  Pyro4.config.SERIALIZER = 'serpent'
  maptask.run()
