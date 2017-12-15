#!/usr/bin/env python
import gevent.monkey
gevent.monkey.patch_all()
import psycogreen.gevent
psycogreen.gevent.patch_psycopg()
import gevent.queue, tweepy, gevent, sys, atexit, traceback
from datetime import datetime
from tools import Printer, Conf
from messages import MESSAGE
from dbrecovery import DBRecoveryWorker
from dbworker import DBWorker
from dbfail import DBFailWorker

QUEUE_SIZE_THRESHOLD = 1000

class Collector(tweepy.StreamListener):
    def main(self, conf):
        # create synchronized printer
        self.printer = Printer('Collector')
        # create queues
        self.queue = gevent.queue.Queue()
        self.db_fail_queue = gevent.queue.Queue()
        self.db_recovery_queue = gevent.queue.Queue()
        # create workers
        self.db_recovery_worker = DBRecoveryWorker(
                    self.db_recovery_queue, self.db_fail_queue, self.queue, conf)
        self.db_fail_worker = DBFailWorker(self.db_fail_queue, self.db_recovery_queue, conf)
        self.db_workers = [ DBWorker(i, self.queue, self.db_fail_queue, conf) \
                            for i in range(conf.num_workers) ]
        # register atexit handler
        atexit.register(self.atexit)
        try:
            # call additional prepare() functions
            self.db_fail_worker.prepare()
            # start workers
            self.db_fail_worker.start()
            self.db_recovery_worker.start()
            [ w.start() for w in self.db_workers ]
            # connect to twitter API
            auth = tweepy.auth.OAuthHandler(conf.API_key, conf.API_secret)
            auth.set_access_token(conf.Access_token, conf.Access_token_secret)
            stream = tweepy.Stream(auth, self, timeout=conf.max_silence)
            self.printer.log("Connected to twitter API.")
            # start collection
            self.last_time = datetime.now()
            stream.filter(**conf.api_filter)
        except KeyboardInterrupt:
            pass
        except Exception, e:
            traceback.print_exc()
        sys.exit()  # this will call self.atexit() below
    def atexit(self):
        # stop workers
        self.printer.log('Stop any remaining worker...')
        for i in range(conf.num_workers):
            self.queue.put((MESSAGE.STOP_COMMAND,))
        self.db_fail_queue.put((MESSAGE.STOP_COMMAND,))
        self.db_recovery_queue.put((MESSAGE.STOP_COMMAND,))
        gevent.wait()
        self.printer.log('Goodbye!')
    # delegate the processing to the workers as much as
    # possible.
    def on_data(self, raw_data):
        if self.queue.qsize() > QUEUE_SIZE_THRESHOLD:
            # it seems workers are no longer working
            return False    # stop
        self.last_time = datetime.now()
        self.printer.log('main: on_data')
        self.queue.put((MESSAGE.DATA, self.last_time, raw_data))
    def on_error(self, status_code):
        self.printer.log('An error has occured! Status code = %s' % status_code)
        return True  # keep stream alive
    def on_timeout(self):
        gevent.sleep(10)
        self.printer.log('Snoozing Zzzzzz')

if __name__ == '__main__':
    conf = Conf()
    collector = Collector()
    collector.main(conf)
