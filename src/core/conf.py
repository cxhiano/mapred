import logging

logging.basicConfig(level=logging.DEBUG)

# The number of tasks a task runner can run concurrently
TASK_RUNNER_SLOTS = 5

# The maximum number of queued task in job runner
JOB_RUNNER_SLOTS = 10

# Task runner will timeout a task will if it cannot finish within this amount
# of time
TASK_RUNNER_TIMEOUT = 5

# Job runner will timeout all tasks if it has not received any report for a
# specific job within this amount of time. It it suggested to set it to be
# greater than TASK_RUNNER_TIMEOUT
JOB_RUNNER_TIMEOUT = 8

# The number of records in a block when splitting input files
RECORDS_PER_BLOCK = 100

# After this number of tasks failed, the job will be deemed as failed
JOB_MAXIMUM_TASK_FAILURE = 20

# Task runner heartbeat will be called every HEARTBEAT_INTERVAL
HEARTBEAT_INTERVAL = 2

# Pyro server thread pool size
PYRO_THREAD_POOL_SIZE = 128
