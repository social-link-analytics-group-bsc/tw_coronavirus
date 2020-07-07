import csv
import logging
import os
import pathlib
import time

from datetime import datetime, timedelta
from data_wrangler import BATCH_SIZE, add_fields
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


def do_update_collection(collection_name, source_collection, end_date, 
                         start_date=None, config_fn=None):                             
    dbm_weekly_collection = DBManager(config_fn=config_fn)
    # Create collection if does not exists
    created_collection = dbm_weekly_collection.create_collection(collection_name)
    if created_collection:
        logging.info('Creating collection: {}...'.format(collection_name))
        dbm_weekly_collection.create_index('id', 'asc', unique=True)
        logging.info('Creating index: id...')
    else:
        logging.info('Setting collection of database to {}'.format(collection_name))
        dbm_weekly_collection.set_collection(collection_name)
    dbm_source = DBManager(collection=source_collection, config_fn=config_fn)
    # If no start date is passed, then use today's date
    if not start_date:
        start_date = datetime.today().strptime('%Y-%m-%d')
    query = {
        'created_at_date':
            {'$gte': start_date, '$lte': end_date}
    }
    logging.info('Searching for tweets between {0} and {1}...'.\
                 format(start_date, end_date))
    tweets_to_copy = dbm_source.search(query)
    logging.info('Going to insert {0:,} tweets into the collection {1}'.\
                 format(tweets_to_copy.count(), collection_name))
    try:
        ret_insertions = dbm_weekly_collection.insert_many_tweets(tweets_to_copy, 
                                                                  ordered=False)
        insertion_counter = ret_insertions.inserted_ids
        logging.info('{0:,} new tweets were inserted into the collection {1}'.\
                     format(insertion_counter, collection_name))
    except Exception as e:
        logging.error('Error when merging {}'.format(e))        


def do_tweets_replication(source_collection, target_collection, start_date, 
                          end_date=None, config_fn=None):
    dbm_source = DBManager(collection=source_collection, config_fn=config_fn)
    dbm_target = DBManager(collection=target_collection, config_fn=config_fn)
    query = {
        'created_at_date':{'$gte': start_date}
    }
    if end_date:
        query['created_at_date'].update(
            {
                '$lte': end_date
            }
        )
    tweets_to_replicate = dbm_source.find_all(query)
    total_tweets = tweets_to_replicate.count()
    logging.info('Replicating {0:,} tweets'.format(total_tweets))
    max_batch = BATCH_SIZE if total_tweets > BATCH_SIZE else total_tweets 
    processing_counter = total_segs = 0
    tweets_to_insert = []
    for tweet in tweets_to_replicate:
        start_time = time.time()
        processing_counter += 1
        tweets_to_insert.append(tweet)
        if len(tweets_to_insert) >= max_batch:
            logging.info('Inserting tweets in the target collection...')
            dbm_target.insert_many(tweets_to_insert)
            tweets_to_insert = []
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_tweets)
        
    
def load_user_demographics(input_file, collection, config_fn=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    users_to_update = []
    processing_counter = total_segs = 0
    with open(input_file, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        rows = list(csv_reader)
        num_lines= len(rows)
        for row in rows:
            start_time = time.time()
            processing_counter += 1
            if len(users_to_update) >= BATCH_SIZE:
                logging.info('Updating users...')
                ret = dbm.bulk_update(users_to_update)
                modified_users = ret.bulk_api_result['nModified']
                logging.info('Updated {0:,} users'.format(modified_users))
                users_to_update = []
            user_id = row['id']
            del row['id']
            row['prediction'] = 'succeded'
            users_to_update.append(
                {
                    'filter': {'id_str': user_id},
                    'new_values': row
                }
            )
            total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        num_lines)
    if len(users_to_update) >= BATCH_SIZE:
        logging.info('Updating users...')
        ret = dbm.bulk_update(users_to_update)
        modified_users = ret.bulk_api_result['nModified']
        logging.info('Updated {0:,} users'.format(modified_users))