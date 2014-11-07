import logging
from core.taskrunner import TaskRunner

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    node = TaskRunner('conf/task_runner.xml')
    node.serve()
