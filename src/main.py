import sys
import example.wordcount as wordcount
from client import Client
from utils.conf_loader import load_config

if __name__ == '__main__':
    client = Client(load_config(sys.argv[1]))

    client.upload('input_1.txt', open('client.py', 'r'))
    client.upload('input_2.txt', open('client.py', 'r'))

    jobconf1 = {
        'mapper': wordcount.map,
        'reducer': wordcount.reduce,
        'cnt_reducers': 2,
        'inputs': ['input_1.txt', 'input_2.txt'],
        'output_dir': 'output_1'
    }

    jobconf2 = {
        'mapper': wordcount.map,
        'reducer': wordcount.reduce,
        'cnt_reducers': 1,
        'inputs': ['input_1.txt', 'input_2.txt'],
        'output_dir': 'output_2'
    }
    client.submit(jobconf2)
    client.submit(jobconf1)
