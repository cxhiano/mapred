import xml.etree.ElementTree as ET

def load_config(filename):
    conf = {}
    tree = ET.parse(filename)
    for node in tree.getroot():
        conf[node.tag] = node.text
    return conf
