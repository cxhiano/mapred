import logging
from dfs.datanode import DataNode

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    node = DataNode('conf/data_node.xml')
    node.run()
