#!/usr/bin/env python
import psycopg2, tweepy, threading, time, json, sys
from utc import TZ_UTC
from debug import debug_console
from Queue import Queue
from datetime import datetime

LONGITUDE = 0
LATITUDE = 1
STOP_COMMAND = 0
QUEUE_SIZE_THRESHOLD = 1000 # approx. 12s collecting

class Conf(object):
    def __init__(self):
        with open('conf.json') as f:
            self.__dict__ = json.load(f)

class SyncPrinter(object):
    def __init__(self):
        self.lock = threading.Lock()
    def log(self, message):
        with self.lock:
            print("%s - %s" % (time.ctime(), str(message)))

class Collector(tweepy.StreamListener):
    def main(self, conf):
        # create synchronized printer        
        self.printer = SyncPrinter()

        # create queue
        self.queue = Queue()
        
        # create db workers
        self.db_workers = [ DBWorker(i, self.queue, self.printer, conf) \
                            for i in range(conf.num_workers) ]
        try:
            # start db workers
            [ w.start() for w in self.db_workers ]
            
            # connect to twitter API
            auth = tweepy.auth.OAuthHandler(conf.API_key, conf.API_secret)
            auth.set_access_token(conf.Access_token, conf.Access_token_secret)
            stream = tweepy.Stream(auth, self, timeout=5)
            self.printer.log("Connected to twitter API.")
            
            # start collection
            stream.filter(**conf.api_filter)
        except KeyboardInterrupt:
            pass
        except Exception, e:
            self.printer.log(e)
            #debug_console(collector=self, LONGITUDE = LONGITUDE,
            #                      LATITUDE = LATITUDE)
        # stop db workers
        self.printer.log('Stop any remaining db worker...')
        for i in range(conf.num_workers):
            self.queue.put(STOP_COMMAND)
        self.printer.log('\nGoodbye!')
        sys.exit()

    # delegate the processing to the workers as much as
    # possible.
    def on_data(self, raw_data):
        if self.queue.qsize() > QUEUE_SIZE_THRESHOLD:
            # it seems workers are no longer working
            return False    # stop
        self.queue.put((datetime.now(), raw_data))

    def on_error(self, status_code):
        self.printer.log('An error has occured! Status code = %s' % status_code)
        return True  # keep stream alive

    def on_timeout(self):
        self.printer.log('Snoozing Zzzzzz')

        

class DBWorker(threading.Thread, tweepy.StreamListener):
    
    def __init__(self, thread_num, queue, printer, conf):
        threading.Thread.__init__(self)
        tweepy.StreamListener.__init__(self)
        self.name = "DBWorker #%d" % thread_num
        self.queue = queue
        self.printer = printer
        self.conf = conf
        
    def run(self):
        self.conn = psycopg2.connect(
                host=self.conf.db_host,
                dbname=self.conf.db_name, 
                user=self.conf.db_user,
                password=self.conf.db_password)
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

    def get_place_bounding_box_as_geojson(self, tweet):
        # we cannot efficiently use tweepy's BoundingBox object for that,
        # we have to use the unaltered json object
        return json.dumps(tweet._json['place']['bounding_box'])

    def get_place_id_as_bigint(self, place):
        # place id is given as a 16-hex-chars string.
        # that's exactly 64bits, thus it can be stored as a bigint
        # in the postgresql database.
        # but bigint is signed, thus we have to shift the value
        # to make it match the allowed range of bigint.
        return eval('0x' + place.id + ' - 0x8000000000000000')

    def on_status(self, tweet):
            self.last_tweet = tweet # for debugging purpose
            coordinates = self.get_coordinates(tweet)
            if tweet.place:
                place_id = self.get_place_id_as_bigint(tweet.place)
                place_bbox = self.get_place_bounding_box_as_geojson(tweet)
                place_type = tweet.place.place_type
                place_name = tweet.place.full_name
            else:
                place_id, place_bbox, place_type, place_name = \
                    None, None, None, None
            user = tweet.user
            tweet.created_at = tweet.created_at.replace(tzinfo = TZ_UTC)
            user.created_at = user.created_at.replace(tzinfo = TZ_UTC)
            self.store_tweet_in_db(  tweet_id = tweet.id,
                                         text = tweet.text,
                                   created_at = tweet.created_at,
                                  captured_at = self.captured_at,
                                     latitude = coordinates[LATITUDE],
                                    longitude = coordinates[LONGITUDE],
                                         lang = tweet.lang,
                                      user_id = user.id,
                              user_utc_offset = user.utc_offset,
                                user_location = user.location,
                              user_created_at = user.created_at,
                         user_followers_count = user.followers_count,
                          user_statuses_count = user.statuses_count,
                           user_friends_count = user.friends_count,
                        user_favourites_count = user.favourites_count,
                                user_lang_str = user.lang,
                                     place_id = place_id,
                                   place_type = place_type,
                                   place_name = place_name,
                                   place_bbox = place_bbox,
                                 collector_id = self.conf.collector_id)

    def store_tweet_in_db(self, **kwargs):
        query = 'SELECT registerTweetAndMetadata(' + \
                    '%(tweet_id)s, ' + \
                    '%(user_id)s, ' + \
                    '%(text)s, ' + \
                    '%(latitude)s, ' + \
                    '%(longitude)s, ' + \
                    '%(created_at)s, ' + \
                    '%(captured_at)s, ' + \
                    '%(lang)s, ' + \
                    '%(user_utc_offset)s, ' + \
                    '%(user_location)s, ' + \
                    '%(user_created_at)s, ' + \
                    '%(user_followers_count)s, ' + \
                    '%(user_statuses_count)s, ' + \
                    '%(user_friends_count)s, ' + \
                    '%(user_favourites_count)s, ' + \
                    '%(user_lang_str)s, ' + \
                    '%(place_id)s, ' + \
                    '%(place_type)s, ' + \
                    '%(place_name)s, ' + \
                    '%(place_bbox)s, ' + \
                    '%(collector_id)s ' + \
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
    conf = Conf()
    collector = Collector()
    collector.main(conf)

