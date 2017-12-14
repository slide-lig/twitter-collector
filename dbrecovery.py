#!/usr/bin/env python
import cPickle as pickle, os, gzip, gevent
from messages import MESSAGE
from tools import Worker, Printer

class DBRecoveryWorker(Worker):
    def __init__(self, db_recovery_queue, db_fail_queue, queue, conf):
        self.db_recovery_queue = db_recovery_queue
        self.db_fail_queue = db_fail_queue
        self.queue = queue
        self.printer = Printer('DB-RECOVERY')
        self.db_reco_filename = "DB-RECOVERY-" + str(conf.collector_id) + ".data.gz"
    def run(self):
        while True:
            # wait for next recovery request
            item = self.db_recovery_queue.get()
            if item[0] == MESSAGE.STOP_COMMAND:
                break
            # do it
            self.recover()
            # notify db_fail greenlet
            self.db_fail_queue.put((MESSAGE.RECOVERY_DONE,))
    def recover(self):
        self.printer.log('Starting recovery.')
        with gzip.GzipFile(self.db_reco_filename, 'rb') as f:
            delay = 0.2
            while True:
                if self.queue.qsize() > 0:
                    # we might be pushing too fast
                    delay = delay * 2
                else:
                    # we can push faster
                    delay = delay / 2
                try:
                    info = pickle.load(f)
                except EOFError:
                    # end of the file, we are done
                    break
                self.queue.put((MESSAGE.RECOVERY_DATA, info))
                gevent.sleep(delay)
        os.remove(self.db_reco_filename)
        self.printer.log('Done.')
