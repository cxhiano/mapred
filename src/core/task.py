from multiprocessing import Process
from core.configurable import Configurable
from utils.rmi import retrieve_object

class Task(Configurable):
    def __init__(self, task_conf):
        super(Task, self).__init__(task_conf)
        #self.process = Process(target=self.run)
        #self.jobrunner = retrieve_object(jobrunner)

    def kill(self):
        self.process.terminate()
