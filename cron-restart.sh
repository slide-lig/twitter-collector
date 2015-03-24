#!/bin/bash
# add as a cron job:
# # crontab -e
# add the following line:
# 30 2 * * * /[...]/cron-restart.sh
kill $(ps -ef | grep collector.py | grep -v grep | awk '{print $2}')
