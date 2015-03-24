#!/usr/bin/env python
# add as a cron job:
# # crontab -e
# add the following line:
# 30 3 * * * /[...]/periodic-stats-update.py
import psycopg2, psycopg2.extras, sys
from datetime import datetime

DB_NAME = 'twitterdb'
DB_USER = 'twitter'

class StatsUpdater(object):
    
    def log(self, s, prefix="StatsUpdater: "):
        if prefix:
            s = prefix + s
        sys.stdout.write(s)
        sys.stdout.flush()

    def run(self):
        self.conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER)
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        try:
            self.log("Connected to database.\n")
            last_stats = self.get_last_stats_record()
            now = datetime.now()
            self.log("Computing num tweets emitted... ")
            num_tweets_emitted = self.get_num_tweets_emitted()
            self.log("done\n", prefix=None)
            self.log("Computing num tweets captured... ")
            num_tweets_captured = self.get_num_tweets_captured(last_stats, now)
            self.log("done\n", prefix=None)
            self.update_stats(last_stats['period_id'], 
                    now, num_tweets_emitted, num_tweets_captured)
            self.log("Stats saved in database.\n")
        finally:
            self.cursor.close()
            self.conn.close()

    def get_1st_record(self, query, query_params=()):
        self.cursor.execute(query, query_params)
        return self.cursor.fetchall()[0] 

    def get_last_stats_record(self):
        query = """ select * from stats_per_period 
                    where period_id = (
                            select max(period_id) from stats_per_period);"""
        return self.get_1st_record(query)

    def get_num_tweets_emitted(self):
        """return the number of tweets emitted since the last check period"""
        # sum the numbers we reported in previous periods
        query = """ select sum(num_tweets_emitted) from stats_per_period;"""
        old_count = self.get_1st_record(query)[0]
        # get the number of statuses emitted by users from the time
        # we started to collect, using the 'statuses_count' information
        query = """ select sum(l.statuses_count - f.statuses_count +1) 
                    from twitter_user u, user_stats f, user_stats l 
                    where u.stats_first_seen = f.user_stats_id 
                    and          u.stats_now = l.user_stats_id;"""
        new_count = self.get_1st_record(query)[0]
        return new_count - old_count

    def get_num_tweets_captured(self, last_stats, now):
        """return the number of tweets captured since the last check period"""
        query = """ select count(*) from tweet
                    where created_at >= %s
                    and created_at < %s"""
        params = (last_stats['end_of_period'], now)
        return self.get_1st_record(query, params)[0]

    def update_stats(self, last_period_id, now, 
                    num_tweets_emitted, num_tweets_captured):
        query = """ insert into stats_per_period(
                        last_period_id,
                        end_of_period,
                        num_tweets_emitted,
                        num_tweets_captured) 
                    values (%s,%s,%s,%s);"""
        self.cursor.execute(query, (last_period_id, now, 
                    num_tweets_emitted, num_tweets_captured)) 
        self.conn.commit()
        return

if __name__ == '__main__':
    updater = StatsUpdater()
    updater.run()

