import sys
import example.wordcount as wordcount
from client import Client
from utils.conf_loader import load_config

if __name__ == '__main__':
    client = Client(load_config(sys.argv[1]))

    client.upload('a.txt', open('client.py', 'r'))
    client.upload('b.txt', open('client.py', 'r'))

    jobconf1 = {
        'mapper': wordcount.map,
        'reducer': wordcount.reduce,
        'cnt_reducers': 2,
        'inputs': ['a.txt', 'b.txt'],
        'output_dir': 'mytask2'
    }

    jobconf2 = {
        'mapper': wordcount.map,
        'reducer': wordcount.reduce,
        'cnt_reducers': 1,
        'inputs': ['a.txt', 'b.txt'],
        'output_dir': 'mytask3'
    }
    client.submit(jobconf2)
    client.submit(jobconf1)
