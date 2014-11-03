from mrio.collector import OutputCollector
from mrio.record_reader import RecordReader
from mrio.record_file import RecordFile
from core.maptask import MapTask
from example.wordcount import WordCount
import Pyro4

if __name__ == '__main__':
    name_node = Pyro4.Proxy('PYRONAME:NameNode')
    t = MapTask()
    f = RecordFile('main.py', name_node)
    r = RecordReader(f)
    collector = OutputCollector(None)
    t.run(WordCount, r, collector)
