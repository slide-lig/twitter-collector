#!/usr/bin/env python
import time, json, sys, traceback
from gevent import Greenlet

class Conf(object):
    def __init__(self):
        with open('conf.json') as f:
            self.__dict__ = json.load(f)

class Printer(object):
    def __init__(self, name):
        self.name = name
        self.last_message = None
        self.repeated = 0
    def log(self, message):
        if message == self.last_message:
            if self.repeated == 0:
                self.formatted_print('repeating last message...')
            self.repeated += 1
        else:
            if self.repeated > 0:
                self.formatted_print('last message was repeated %d times.' % \
                        (self.repeated +1))
            self.repeated = 0
            self.formatted_print(message)
            self.last_message = message
    def formatted_print(self, s):
        print("%s %s: %s" % (time.ctime(), self.name, str(s)))

class Worker:
    def start(self):
        Greenlet.spawn(self.protected_run)
    def protected_run(self):
        try:
            self.run()
        except:
            traceback.print_exc()
            sys.exit()

