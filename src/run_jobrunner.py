import logging
from core.jobrunner import JobRunner

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    node = JobRunner('conf/job_runner.xml')
    node.serve()
