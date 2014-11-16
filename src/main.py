import sys
import example
from client import Client
from utils.conf_loader import load_config

if __name__ == '__main__':
    client = Client(load_config(sys.argv[1]))

    if len(sys.argv) > 2:
        # with open('output_1.txt', 'w') as f:
        #     client.download()
    else:
        client.upload('input_1.txt', open('client.py', 'r'))
        client.upload('input_2.txt', open('client.py', 'r'))

        jobconf1 = {
            'mapper': example.wordcount.map,
            'reducer': example.wordcount.reduce,
            'cnt_reducers': 2,
            'inputs': ['input_1.txt', 'input_2.txt'],
            'output_dir': 'output_1'
        }

        jobconf2 = {
            'mapper': example.slow_wordcount.map,
            'reducer': example.slow_wordcount.reduce,
            'cnt_reducers': 2,
            'inputs': ['input_1.txt', 'input_2.txt'],
            'output_dir': 'output_2'
        }
        client.submit(jobconf2)
        client.submit(jobconf1)
