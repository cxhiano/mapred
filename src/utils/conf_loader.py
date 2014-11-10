""" This module provide helper function to load config from xml files """
import xml.etree.ElementTree as ET

def _xml2dict(node, dict_):
    """ Recursively transform a xml element tree rooted at node to a dict """
    for child in node.getchildren():
        children = child.getchildren()
        if len(children) == 0:
            dict_[child.tag] = child.text
        else:
            tmp = {}
            _xml2dict(child, tmp)
            dict_[child.tag] = tmp

def load_config(filename):
    """ Load config from xml file and transform it to a dict """
    conf = {}
    tree = ET.parse(filename)
    _xml2dict(tree.getroot(), conf)
    return conf
