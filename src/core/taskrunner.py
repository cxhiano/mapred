from Queue import Queue
from core.configurable import Configurable
from core.maptask import MapTask
from utils.conf_loader import load_config
import utils.serialize as serialize

class TaskRunner:
    def __init__(self, conf):
        self.load_dict(load_config(conf))

    def serve(self):
        self.ns = locateNS(self.pyroNS['host'], int(self.pyroNS['port']))

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.namenode)
        self.jobrunner = retrieve_object(self.ns, self.jobrunner)

        while True:
            task_conf = serialize.loads(self.jobrunner.get_task())
            maptask = MapTask(task_conf, self)
            try:
                maptask.run()
            except:
                logging.info('map task %d for job %d failed' % \
                    (task_conf.taskid, task_conf.jobid))

            logging.info('map task %d for job %d completed' % \
                (task_conf.taskid, task_conf.jobid))
