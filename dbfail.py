#!/usr/bin/env python
import cPickle as pickle, os, gzip
from messages import MESSAGE
from tools import Worker, Printer

class DBFailWorker(Worker):
    def __init__(self, db_fail_queue, db_recovery_queue, conf):
        self.db_fail_queue = db_fail_queue
        self.db_recovery_queue = db_recovery_queue
        self.printer = Printer('DB-FAIL')
        self.f = None
        self.db_fail_filename = "DB-FAIL-" + str(conf.collector_id) + ".data.gz"
        self.db_reco_filename = "DB-RECOVERY-" + str(conf.collector_id) + ".data.gz"
        self.recovering = False
    def prepare(self):
        # if there was some remaining failure pending from previous run,
        # open the failures file (this should trigger a recovery process
        # as soon as we get a DB_CONNECTION_OK message).
        if os.path.exists(self.db_fail_filename):
            self.ensure_file_ready()
    def ensure_file_ready(self):
        if self.f is None:
            self.f = gzip.GzipFile(self.db_fail_filename, "ab")
    def start_recovery(self):
        self.recovering = True
        # we close the file here, and will re-open it later in case of
        # new failures, in order to start a new file.
        self.f.close()
        self.f = None
        # prepare recovery by renaming the file, and notify the
        # recovery greenlet.
        os.rename(self.db_fail_filename, self.db_reco_filename)
        self.db_recovery_queue.put((MESSAGE.RECOVERY_START,))
    def run(self):
        while True:
            item = self.db_fail_queue.get()
            if item[0] == MESSAGE.STOP_COMMAND:
                break
            elif item[0] == MESSAGE.DB_CONNECTION_OK:
                # if we have registered db failures and recovery is not already running,
                # this is the right time to start the recovery.
                if self.f is not None and self.recovering == False:
                    self.printer.log('DB connection is back.')
                    self.start_recovery()
            elif item[0] == MESSAGE.RECOVERY_DONE:
                self.recovering = False
            elif item[0] == MESSAGE.FAILURE_DATA:
                e, info = item[1:]
                self.printer.log(str(e))
                self.ensure_file_ready()
                pickle.dump(info, self.f)
                self.f.flush()
            else:
                raise Exception('Unknown command.')
    def __del__(self):
        if self.f is not None:
            self.f.close()
            self.printer.log('DB failures occured: some tweets were saved to %s.' \
                                % self.db_fail_filename)
