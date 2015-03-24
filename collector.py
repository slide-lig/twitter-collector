#!/usr/bin/env python
import psycopg2, tweepy, threading, time
from utc import TZ_UTC
from debug import debug_console
from Queue import Queue
from datetime import datetime

API_key=WRITE_YOUR_API_KEY_HERE
API_secret=WRITE_YOUR_API_SECRET_HERE
Access_token=WRITE_YOUR_ACCESS_TOKEN_HERE
Access_token_secret=WRITE_YOUR_ACESS_TOKEN_SECRET_HERE
DB_NAME = 'twitterdb'
DB_USER = 'twitter'
LONGITUDE = 0
LATITUDE = 1
NUM_DB_WORKERS = 20
STOP_COMMAND = 0

class SyncPrinter(object):
    def __init__(self):
        self.lock = threading.Lock()
    def log(self, message):
        with self.lock:
            print("%s - %s" % (time.ctime(), str(message)))

class Collector(tweepy.StreamListener):
    def main(self):
        # create synchronized printer        
        self.printer = SyncPrinter()

        # create queue
        self.queue = Queue()
        
        # create db workers
        self.db_workers = [ DBWorker(i, self.queue, self.printer) \
                            for i in range(NUM_DB_WORKERS) ]
        try:
            # start db workers
            [ w.start() for w in self.db_workers ]
            
            # connect to twitter API
            auth = tweepy.auth.OAuthHandler(API_key, API_secret)
            auth.set_access_token(Access_token, Access_token_secret)
            stream = tweepy.Stream(auth, self, timeout=5)
            self.printer.log("Connected to twitter API.")
            
            # start collection
            stream.filter(locations=[-180,-90,180,90])
        except KeyboardInterrupt:
            pass
        except Exception, e:
            self.printer.log(e)
            #debug_console(collector=self, LONGITUDE = LONGITUDE,
            #                      LATITUDE = LATITUDE)
        # stop db workers
        self.printer.log('Stop any remaining db worker...')
        for i in range(NUM_DB_WORKERS):
            self.queue.put(STOP_COMMAND)
        self.printer.log('\nGoodbye!')

    # delegate the processing to the workers as much as
    # possible.
    def on_data(self, raw_data):
        self.queue.put((datetime.now(), raw_data))

    def on_error(self, status_code):
        self.printer.log('An error has occured! Status code = %s' % status_code)
        return True  # keep stream alive

    def on_timeout(self):
        self.printer.log('Snoozing Zzzzzz')

        

class DBWorker(threading.Thread, tweepy.StreamListener):
    
    def __init__(self, thread_num, queue, printer):
        threading.Thread.__init__(self)
        tweepy.StreamListener.__init__(self)
        self.name = "DBWorker #%d" % thread_num
        self.queue = queue
        self.printer = printer
        
    def run(self):
        self.conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER)
        self.cursor = self.conn.cursor()
        try:
            self.printer.log("%s: Connected to database." % self.name)
            running = True
            while running:
                item = self.queue.get()
                if item == STOP_COMMAND:
                    running = False
                else:
                    self.captured_at, raw_data = item
                    # use the on_data() method of base class StreamListener
                    # on_data() should call on_status() or similar methods 
                    # defined below
                    self.on_data(raw_data)
                self.queue.task_done()
        finally:
            self.cursor.close()
            self.conn.close()

    def get_coordinates(self, tweet):
        if tweet.coordinates == None:
            return {    LATITUDE: None,
                        LONGITUDE: None }
        else:
            return tweet.coordinates['coordinates']

    def on_status(self, tweet):
            self.last_tweet = tweet # for debugging purpose
            coordinates = self.get_coordinates(tweet)
            user = tweet.user
            tweet.created_at = tweet.created_at.replace(tzinfo = TZ_UTC)
            user.created_at = user.created_at.replace(tzinfo = TZ_UTC)
            self.store_tweet_in_db(  tweet_id = tweet.id,
                                         text = tweet.text,
                              favourite_count = tweet.favorite_count,
                                retweet_count = tweet.retweet_count,
                                   created_at = tweet.created_at,
                                  captured_at = self.captured_at,
                                     latitude = coordinates[LATITUDE],
                                    longitude = coordinates[LONGITUDE],
                                      user_id = user.id,
                              user_utc_offset = user.utc_offset,
                                user_location = user.location,
                              user_created_at = user.created_at,
                         user_followers_count = user.followers_count,
                          user_statuses_count = user.statuses_count,
                           user_friends_count = user.friends_count,
                        user_favourites_count = user.favourites_count,
                                user_lang_str = user.lang)

    def store_tweet_in_db(self, **kwargs):
        query = 'SELECT registerTweet(' + \
                    '%(tweet_id)s, ' + \
                    '%(user_id)s, ' + \
                    '%(text)s, ' + \
                    '%(favourite_count)s, ' + \
                    '%(latitude)s, ' + \
                    '%(longitude)s, ' + \
                    '%(retweet_count)s, ' + \
                    '%(created_at)s, ' + \
                    '%(captured_at)s, ' + \
                    '%(user_utc_offset)s, ' + \
                    '%(user_location)s, ' + \
                    '%(user_created_at)s, ' + \
                    '%(user_followers_count)s, ' + \
                    '%(user_statuses_count)s, ' + \
                    '%(user_friends_count)s, ' + \
                    '%(user_favourites_count)s, ' + \
                    '%(user_lang_str)s ' + \
                ')' 
        self.cursor.execute(query, kwargs)   
        self.conn.commit()

    def on_limit(self, track):
        self.printer.log('Limitation notice: skipped %s tweets' % str(track))
        return

    def on_disconnect(self, notice):
        # disconnect codes here: https://dev.twitter.com/docs/streaming-apis/messages#Disconnect_messages_disconnect
        self.printer.log('Disconnect notice: %s' % str(notice))
        return

    def on_event(self, status):
        self.printer.log('Event: %s' % str(status))
        return


if __name__ == '__main__':
    collector = Collector()
    collector.main()

