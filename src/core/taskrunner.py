from Queue import Queue
from core.maptask import MapTask
from utils.conf_loader import load_config

class TaskRunner:
    def __init__(self, conf):
        self.tasks = Queue()
        self.conf = load_config(conf)

    def serve(self):
        self.ns = locateNS(**self.conf['pyroNS'])

        if self.ns is None:
            logging.error('Cannot locate Pyro name server')
            return

        self.namenode = retrieve_object(self.ns, self.conf['namenode'])
        self.jobrunner = retrieve_object(self.ns, self.conf['jobrunner'])

        while True:
            context = Context(self.jobrunner.get_task())
            context.namenode = self.namenode
            maptask = MapTask(context)
            try:
                maptask.run()
            except:
                logging.info('map task %d for job %d failed' % \
                    (context.taskid, context.jobid))

            logging.info('map task %d for job %d completed' % \
                (context.taskid, context.jobid))
