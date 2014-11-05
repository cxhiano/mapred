from mrio.collector import OutputCollector
from mrio.record_reader import RecordReader
from mrio.record_file import RecordFile
from core.maptask import MapTask
from core.context import Context
from utils.rmi import *
from example.wordcount import WordCount
import Pyro4

def create_input(name_node):
    fname = 'a.txt'
    content = '''
              Logging is a means of tracking events that happen when some software runs.
              The software's developer adds logging calls to their code to indicate that
              certain events have occurred. An event is described by a descriptive message
              which can optionally contain variable data (i.e. data that is potentially
              different for each occurrence of the event). Events also have an importance
              which the developer ascribes to the event; the importance can also be called
              the level or severity.
              '''

    datanode = name_node.create_file(fname)
    datanode.write_file(fname, content)
    datanode.close_file(fname)

if __name__ == '__main__':
    ns = Pyro4.locateNS(port=8888)
    name_node = retrieve_object(ns, 'NameNode')
    create_input(name_node)
    context = Context(2, 10, 2, WordCount, WordCount)
    context.name_node = name_node
    context.input = 'a.txt'
    task = MapTask(2, context)
    task.run()
    print name_node.get_file('2_2_0')
    task.cleanup()
    name_node.get_file('2_2_0')
