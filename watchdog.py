#!/usr/bin/env python
import multiprocessing, heapq, os, signal
from Queue import Empty as QueueEmpty
from datetime import datetime, timedelta

WATCHDOG_TIMEOUT = 10

class WatchDog(multiprocessing.Process):
    def __init__(self, queue):
        multiprocessing.Process.__init__(self)
        self.queue = queue
        self.monitoring = []
        self.phase = 0
    def run(self):
        while self.phase < 2:
            timeout = None
            if self.phase == 1:
                timeout = max(0, (self.monitoring[0][0] - datetime.now()).total_seconds())
            try:
                signed_req_pid = self.queue.get(block=True, timeout=timeout)
                req_pid = abs(signed_req_pid)
            except QueueEmpty:
                failing_pid = heapq.heappop(self.monitoring)[1]
                try:
                    os.kill(failing_pid, signal.SIGKILL)
                    print 'watchdog: killed pid %d' % failing_pid
                except:
                    pass
                if len(self.monitoring) == 0:
                    self.phase = 2 # stop
                continue
            except KeyboardInterrupt:
                continue
            # if we already know this process, remove its previous timeout
            found = None
            for i in range(len(self.monitoring)):
                if self.monitoring[i][1] == req_pid:
                    found = i
                    break
            if found != None:
                del self.monitoring[i]
            if signed_req_pid > 0:
                # insert the new timeout
                heapq.heappush(self.monitoring, (
                    datetime.now() + timedelta(seconds = WATCHDOG_TIMEOUT),
                    req_pid
                ))
                self.phase = 1
            elif len(self.monitoring) == 0:
                self.phase = 0

