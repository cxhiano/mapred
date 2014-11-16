import sys
from example import wordcount, keycount
from client import Client
from utils.conf_loader import load_config

if __name__ == '__main__':
    client = Client(load_config(sys.argv[1]))

    jobconf1 = {
        'mapper': wordcount.map,
        'reducer': wordcount.reduce,
        'cnt_reducers': 2,
        'inputs': ['input_1.txt', 'input_2.txt'],
        'output_dir': 'output_1'
    }

    jobconf2 = {
        'mapper': keycount.map,
        'reducer': keycount.reduce,
        'cnt_reducers': 1,
        'inputs': ['input_1.txt', 'input_2.txt'],
        'output_dir': 'output_2'
    }
    client.submit(jobconf1)
    client.submit(jobconf2)
