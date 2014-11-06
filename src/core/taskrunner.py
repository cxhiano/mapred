from queue import Queue

class TaskRunner:
    def __init__(self):
        self.tasks = Queue()

    def add_task(self, taskid, context):
        self.tasks.put((taskid, context))

    def serve(self):
        while True:
            taskid, context = self.tasks.get()
