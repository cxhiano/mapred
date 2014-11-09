import Pyro4
from core.configurable import Configurable
from utils.rmi import retrieve_object

class Task(Configurable):
    def __init__(self, task_conf):
        super(Task, self).__init__(task_conf)
        ns = Pyro4.locateNS()
        self.jobrunner = retrieve_object(ns, self.jobrunner)
