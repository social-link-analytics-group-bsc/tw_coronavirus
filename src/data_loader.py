import csv
import logging
import os
import pathlib
import time

from datetime import timedelta
from utils.db_manager import DBManager
from utils.utils import calculate_remaining_execution_time, SPAIN_LANGUAGES, \
                         get_spain_places_regex

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


def upload_tweet_sentiment():
    print('Process started, it can take several time. Follow updates through ' \
          'the log...')
    dbm_local = DBManager(collection='tweets_esp_hpai')
    dbm_remote = DBManager(collection='bsc-ls', 
                           config_fn='config_mongo_hpai.json')
    query = {'$and': [{'lang': {'$in': SPAIN_LANGUAGES}},
                      {'$or': [{'place.country': 'Spain'}, 
                               {'user.location': {'$in': \
                                   get_spain_places_regex()}}]}]}
    query.update({'retweeted_status': {'$exists': 0}})
    query.update({'sentiment_score': {'$exists': 1}})
    tweets = dbm_local.search(query)
    total_tweets = tweets.count()
    processing_counter = total_segs = modified_records = found_tweets = 0
    logging.info('Going to upload {0:,} tweets'.format(total_tweets))
    for tweet in tweets:
        start_time = time.time()
        processing_counter += 1
        logging.info('[{0}/{1}] Processing tweet:\n{2}'.\
                     format(processing_counter, total_tweets, tweet['id']))
        sentiment_dict = {
            'sentiment': {
                'score': tweet['sentiment_score'] if tweet['sentiment_score_polyglot'] else None,                
            }            
        }
        if tweet['sentiment_score_polyglot']:
            sentiment_dict['sentiment']['raw_score_polyglot'] = \
                tweet['sentiment_score_polyglot']
        if 'sentiment_score_sentipy' in tweet:
            sentiment_dict['sentiment']['raw_score_sentipy'] = \
                tweet['sentiment_score_sentipy']
        if 'sentiment_score_affin' in tweet:
            sentiment_dict['sentiment']['raw_score_affin'] = \
                tweet['sentiment_score_affin']
        ret_update = dbm_remote.update_record({'id': int(tweet['id'])}, 
                                               sentiment_dict)
        if ret_update.matched_count == 0:
            logging.info('Could not find in the remote server a tweet with ' \
                         'the id {}'.format(tweet['id']))
        elif ret_update.matched_count == 1:
            found_tweets += 1
            if ret_update.modified_count == 0:
                logging.info('Found tweet but did not update.')
            elif ret_update.modified_count == 1:
                modified_records += 1
                logging.info('Remote tweet update with sentiment info!')
        total_segs += calculate_remaining_execution_time(start_time, total_segs, 
                                                         processing_counter, 
                                                         total_tweets)
    logging.info('Total processed tweets: {0:,}\n'\
                 'Total found tweets in remote server: {1:,}\n'
                 'Total updated tweets in remote server: {2:,}\n'.\
                 format(total_tweets, found_tweets, modified_records))
    print('Process finished!')


def do_collection_merging(master_collection, collections_to_merge, 
                          config_fn=None):
    dbm_master = DBManager(collection=master_collection, config_fn=config_fn)
    for collection in collections_to_merge:
        logging.info('Merging collection {0} into {1}'.format(collection, master_collection))
        dbm_collection_to_merge = DBManager(collection=collection, config_fn=config_fn)
        tweets = dbm_collection_to_merge.find_all()
        logging.info('Trying to insert {0:,} tweets'.format(tweets.count()))
        try:
            ret_insertions = dbm_master.insert_many_tweets(tweets, ordered=False)
            insertion_counter = ret_insertions.inserted_ids
            logging.info('{0:,} new tweets were inserted into the collection {1}'.\
                         format(insertion_counter, master_collection))
        except Exception as e:
            logging.error('Error when merging {}'.format(e))        
