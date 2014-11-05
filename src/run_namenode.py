import logging
from dfs.namenode import NameNode

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    node = NameNode('conf/name_node.xml')
    node.run()
