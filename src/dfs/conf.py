import logging

logging.basicConfig(level=logging.DEBUG)

# The period of health checks performed by name node
NAMENODE_HEALTH_CHECK_INTERVAL = 2

# The number of replicas when creating a new file
REPLICATION_LEVEL = 2