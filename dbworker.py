import psycopg2, tweepy, time, json
from utc import TZ_UTC
from messages import MESSAGE
from tools import Worker, Printer

LONGITUDE = 0
LATITUDE = 1
DB_RECONNECT_DELAY = 20

SQL_QUERY = 'SELECT registerTweetAndMetadata(' + \
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

class DBWorker(tweepy.StreamListener, Worker):
    def __init__(self, thread_num, queue, db_fail_queue, conf):
        tweepy.StreamListener.__init__(self)
        self.queue = queue
        self.db_fail_queue = db_fail_queue
        self.printer = Printer("DBWorker #%d" % thread_num)
        self.conf = conf
        self.conn = None
        self.cursor = None
        self.last_connect_attempt = 0
    def connected(self):
        return self.cursor is not None
    def disconnect(self):
        self.cursor.close()
        self.cursor = None
        self.conn.close()
        self.conn = None
    def connect(self):
        self.last_connect_attempt = time.time()
        self.conn = psycopg2.connect(
                host=self.conf.db_host,
                dbname=self.conf.db_name,
                user=self.conf.db_user,
                password=self.conf.db_password)
        self.cursor = self.conn.cursor()
        self.printer.log("Connected to database.")
        self.db_fail_queue.put((MESSAGE.DB_CONNECTION_OK,))
    def run(self):
        try:
            while True:
                item = self.queue.get()
                if item[0] == MESSAGE.STOP_COMMAND:
                    break
                elif item[0] == MESSAGE.DATA:
                    self.captured_at, raw_data = item[1:]
                    # use the on_data() method of base class StreamListener
                    # on_data() should call on_status() or similar methods
                    # defined below
                    self.printer.log('on_data')
                    self.on_data(raw_data)
                elif item[0] == MESSAGE.RECOVERY_DATA:
                    self.printer.log('recovery_data')
                    # info is ready for inserting in db directly
                    info = item[1]
                    self.store_tweet_in_db(**info)
                else:
                    raise Exception('Unknown command.')
        finally:
            if self.connected():
                self.disconnect()
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
        info = dict(             tweet_id = tweet.id,
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
        self.store_tweet_in_db(**info)
    def store_tweet_in_db(self, **info):
        if not self.connected() and \
                time.time() - self.last_connect_attempt < DB_RECONNECT_DELAY:
            e = Exception('RECONNECT_DELAY not reached yet')
            self.db_fail_queue.put((MESSAGE.FAILURE_DATA, e, info))
        else:
            try:
                if not self.connected():
                    self.connect()
                self.cursor.execute(SQL_QUERY, info)
                self.conn.commit()
            except BaseException as e:
                self.db_fail_queue.put((MESSAGE.FAILURE_DATA, e, info))
                if self.connected():
                    self.disconnect()
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
