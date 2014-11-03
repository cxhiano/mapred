from mrio.collector import OutputCollector
from mrio.record_reader import RecordReader
from core.maptask import MapTask
from example.wordcount import WordCount
import Pyro4

if __name__ == '__main__':
    name_node = Pyro4.Proxy('PYRONAME:NameNode')
    t = MapTask()
    node = name_node.get_file('xml')
    r = RecordReader(50, 'name_node.xml', node)
    collector = OutputCollector(None)
    t.run(WordCount, r, collector)
