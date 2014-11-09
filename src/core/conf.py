import logging

logging.basicConfig(level=logging.DEBUG)

# The number of tasks a task runner can run concurrently
TASK_RUNNER_SLOTS = 5
# A task will timeout if it cannot finish within this amount of time
TASK_TIMEOUT = 10000

# The maximum number of queued task in job runner
JOB_RUNNER_SLOTS = 1

# The number of records in a block when splitting input files
RECORDS_PER_BLOCK = 5

# After this number of tasks failed, the job will be deemed as failed
JOB_MAXIMUM_TASK_FAILURE = 20

# Task runner heartbeat will be called every HEARTBEAT_INTERVAL
HEARTBEAT_INTERVAL = 2