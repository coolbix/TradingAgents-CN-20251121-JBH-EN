"""Redis keynames and configuration constants used for queue services (centralized definition)
"""

#Redis Key Name Constant
READY_LIST = "qa:ready"

TASK_PREFIX = "qa:task:"
BATCH_PREFIX = "qa:batch:"
SET_PROCESSING = "qa:processing"
SET_COMPLETED = "qa:completed"
SET_FAILED = "qa:failed"
BATCH_TASKS_PREFIX = "qa:batch_tasks:"

#Concurrent Control Related
USER_PROCESSING_PREFIX = "qa:user_processing:"
GLOBAL_CONCURRENT_KEY = "qa:global_concurrent"
VISIBILITY_TIMEOUT_PREFIX = "qa:visibility:"

#Configure Constant - Open Source Limit
DEFAULT_USER_CONCURRENT_LIMIT = 3
GLOBAL_CONCURRENT_LIMIT = 3  #The maximum co-production limit for open source is 3
VISIBILITY_TIMEOUT_SECONDS = 300  #Five minutes.

