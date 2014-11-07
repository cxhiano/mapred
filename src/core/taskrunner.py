import sys
import Pyro4
from Queue import Queue
from core.configurable import Configurable
from core.maptask import MapTask
from utils.conf_loader import load_config
from utils.rmi import *
import utils.serialize as serialize

class TaskRunner(Configurable):
    def __init__(self, conf):
        self.load_dict(load_config(conf))
        Pyro4.config.SERIALIZER = "marshal"

    def serve(self):
        self.ns = locateNS(self.pyroNS['host'], int(self.pyroNS['port']))

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.namenode)
        self.jobrunner = retrieve_object(self.ns, self.jobrunner)

        while True:
            task_conf = serialize.loads(self.jobrunner.get_task())
            logging.info('Got task with config %s' % str(task_conf))
            maptask = MapTask(task_conf, self)

            try:
                maptask.run()
            except:
                logging.info('map task %d for job %d failed: %s' % \
                    (maptask.taskid, maptask.jobid, sys.exc_info()[1]))
                self.jobrunner.report_mapper_fail(maptask.jobid, maptask.taskid)

            logging.info('map task %d for job %d completed' % \
                (maptask.taskid, maptask.jobid))
            self.jobrunner.report_mapper_succeed(maptask.jobid, maptask.taskid)
