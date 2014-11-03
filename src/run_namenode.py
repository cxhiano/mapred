from dfs.namenode import NameNode

if __name__ == '__main__':
    node = NameNode('conf/name_node.xml')
    node.files = { 'main.py': 'DataNode1' }
    node.run()
    '''
    node.health_check()
    f = node.get_file('xml')
    print f.read(0, 100)
    '''
