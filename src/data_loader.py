import csv
import logging
import os
import pathlib
import time

from datetime import timedelta
from utils.db_manager import DBManager
from utils.utils import calculate_remaining_execution_time

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)


def insert_tweets_from_csv():
    dbm = DBManager(collection='tweets')
    data_dir = '../data/bsc/'
    for file_name in os.listdir(data_dir):
        if file_name.endswith('.csv'):
            logging.info('Reading file: {0}'.format(file_name))
            fp_file_name = os.path.join(data_dir,file_name)
            with open(fp_file_name, 'r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:                    
                    dbm.insert_tweet(row)


def upload_tweets():
    dbm_local = DBManager(collection='tweets_esp_hpai')
    dbm_remote = DBManager(collection='bsc-ls', 
                           config_fn='src/config_mongo_hpai.json')
    tweets = dbm_local.search({'$and': [
                                        {'lang': {'$in': ['es', 'ca', 'eu', 'gl']}},
                                        {'$or': [{"place.country": "Spain"}, 
                                                 {'user.location': {'$ne': None}}
                                                ]},
                                        {'retweeted_status': {'$exists': 0}},
                                        {'sentiment_score': {'$exists': 1}}
                                        ]
                               })
    total_tweets = tweets.count()
    processing_counter = total_segs = 0
    logging.info('Going to upload {0:,} tweets'.format(total_tweets))
    for tweet in tweets:
        start_time = time.time()
        processing_counter += 1
        logging.info('[{0}/{1}] Processing tweet:\n{2}'.\
                     format(processing_counter, total_tweets, tweet['id']))
        sentiment_dict = {
            'sentiment_score': tweet['sentiment_score'],
            'sentiment_score_polyglot': tweet['sentiment_score_polyglot'],
            'sentiment_score_sentipy': tweet['sentiment_score_sentipy'],
            'sentiment_score_affin': tweet['sentiment_score_affin']
        }
        ret_update = dbm_remote.update_record({'id': tweet['id']}, sentiment_dict)
        if ret_update.matched_count > 0:
            logging.info('Remote tweet update with sentiment info')
        else:
            logging.info('Could not find remote tweet')
        total_segs += calculate_remaining_execution_time(start_time, total_segs, 
                                                         processing_counter, 
                                                         total_tweets)