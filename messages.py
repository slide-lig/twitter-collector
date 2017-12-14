class MESSAGE:
    STOP_COMMAND = 0
    DATA = 1
    FAILURE_DATA = 2
    RECOVERY_DATA = 3
    DB_CONNECTION_OK = 4
    RECOVERY_START = 5
    RECOVERY_DONE = 6
    QUEUE_SIZE_THRESHOLD = 1000
    DB_RECONNECT_DELAY = 20