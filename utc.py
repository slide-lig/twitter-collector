#!/usr/bin/env python
import datetime

ZERO = datetime.timedelta(0)

class UTC(datetime.tzinfo):
    """UTC"""
    def utcoffset(self, dt):
        return ZERO
    def tzname(self, dt):
        return "UTC"
    def dst(self, dt):
        return ZERO

TZ_UTC = UTC()

