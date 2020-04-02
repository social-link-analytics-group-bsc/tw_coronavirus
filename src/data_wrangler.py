import csv
import logging
import pathlib
import os
import preprocessor as tw_preprocessor
import time

from datetime import datetime, timedelta
from utils.language_detector import detect_language
from utils.db_manager import DBManager
from utils.utils import get_tweet_datetime
from utils.sentiment_analyzer import SentimentAnalyzer


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)



def infer_language(data_folder, input_file_name, sample=False):
    output_file_name = data_folder + '/processing_outputs/tweets_languages_' + input_file_name
    input_file_name = data_folder + '/' + input_file_name
    sample_size = 10

    print('Starting process to infer language of tweets')

    logging.info('Looking for file that contains pre-processed tweet ids...')
    processed_tweet_ids = set()
    try:
        with open(output_file_name) as csv_file:
            logging.info('Found file with existing pre-processed tweet ids...')
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                processed_tweet_ids.add(row['tweet_id'])
    except IOError:
        pass

    logging.info('Infering language of tweets...')
    tweet_langs = []
    try:
        with open(input_file_name, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            row_counter = 0
            for row in csv_reader:            
                if row['tweet_id'] in processed_tweet_ids:
                    # ignore tweets that were already processed
                    continue
                row_counter += 1
                if sample and row_counter > sample_size:
                    break 
                logging.info('[{0}] Infering language of tweet: {1}'.\
                            format(row_counter, row['tweet_id']))
                lang = detect_language(row['tweet'])
                tweet_langs.append(
                    {'tweet_id': row['tweet_id'], 'lang': lang}
                )         
    except Exception as e:
        logging.exception('The following error occurs when infering language '\
                          'of tweets')
    finally:
        if len(processed_tweet_ids) > 0:
            writing_mode = 'a'
        else:
            writing_mode = 'w'
        logging.info('Saving results to file: {}'.format(output_file_name))
        with open(output_file_name, writing_mode) as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=['tweet_id', 'lang'])
            if writing_mode == 'w':                
                csv_writer.writeheader()            
            for tweet_lang in tweet_langs:
                csv_writer.writerow(tweet_lang)
    
    print('Process finishes successfully!')


def add_date_time_field_tweet_objs():
    """
    Add date fields to tweet documents
    """
    dbm = DBManager('tweets')
    tweets = dbm.find_all()
    for tweet in tweets:
        tweet_id = tweet['id']
        logging.info('Generating the datetime of tweet: {}'.format(tweet_id))
        str_tw_dt = tweet['created_at']
        tw_dt, tw_d, tw_t = get_tweet_datetime(str_tw_dt)
        tweet['date_time'] = tw_dt
        tweet['date'] = tw_d
        tweet['time'] = tw_t
        dbm.update_record({'id': tweet_id}, tweet)


def check_datasets_intersection():
    dbm = DBManager(config_fn='config_mongo_hpai.json', collection='tweets')
    data_dir = '../data/bsc/'
    remote_total_tweets = dbm.num_records_collection()
    logging.info('Total tweets in remote database: {0:,}'.format(remote_total_tweets))
    local_total_tweets = 0
    total_intersections = 0
    dt_now_str = datetime.now().strftime("%d-%m-%Y")
    for file_name in os.listdir(data_dir):
        if file_name.endswith('.csv'):
            logging.info('Reading file: {0}'.format(file_name))
            fp_file_name = os.path.join(data_dir,file_name)
            with open(fp_file_name, 'r') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    local_total_tweets += 1
                    tweet_id = row['tweet_id']
                    logging.info('Checking tweet: {}'.format(tweet_id))
                    rec = dbm.find_record({'id': tweet_id})
                    if rec:
                        total_intersections += 1
                        logging.info('Found intersection!, total intersection' \
                                     ' so far: {0:,}'. format(total_intersections))
    s = 'Datetime: {0}\n' \
        'Total Tweets Remote: {1:,}\n' \
        'Total Tweets Local: {2:,}\n' \
        'Interception: {3:,}'.format(dt_now_str, remote_total_tweets, \
                                  local_total_tweets, total_intersections)
    logging.info(s)
    output_file_name = os.path.join(data_dir, 'processing_outputs', 
                                    'dbs_inter_{}.txt'.format(dt_now_str))
    with open(output_file_name, 'w') as output_file:        
        output_file.write(s)


def check_performance_language_detection():
    data_dir = '../data/bsc/'
    tweets_lang_file = data_dir + '/processing_outputs/tweets_languages_ours_2019-01-01_to_2020-02-22_coronavirus_es-en_tweets.csv'
    dbm = DBManager(collection='tweets_es_hpai')
    total_correct = total_intersections = 0
    total_processed_tweets = 0
    with open(tweets_lang_file, 'r') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                total_processed_tweets += 1
                tweet_id = row['tweet_id']
                logging.info('Checking if tweet {} exists'.format(tweet_id))
                tweet_db = dbm.find_record({'id': tweet_id})
                if tweet_db:
                    logging.info('Found tweet!')
                    total_intersections += 1
                    if tweet_db['lang'] == row['lang']:
                        total_correct += 1
    accuracy = 0                        
    if total_intersections > 0:
        accuracy = round(total_correct/total_intersections,2)        
    logging.info('.: Language detection performance :.\n- ' \
                 '= Total processed tweets: {0:,}' \
                 '= Intersection: {1:,} '   
                 '= Accuracy: {2}'.\
                 format(total_processed_tweets, \
                        total_intersections, \
                        accuracy))
                

def compute_sentiment_analysis_tweets():
    dbm = DBManager(collection='tweets_esp_hpai')
    tweets = dbm.search({'retweeted_status': {'$exists': 0}, 
                         'sentiment_score': {'$exists': 0}})
    sa = SentimentAnalyzer()                         
    # set option of preprocessor
    tw_preprocessor.set_options(tw_preprocessor.OPT.URL, 
                                tw_preprocessor.OPT.MENTION, 
                                tw_preprocessor.OPT.HASHTAG,
                                tw_preprocessor.OPT.RESERVED,
                                tw_preprocessor.OPT.NUMBER,
                                tw_preprocessor.OPT.EMOJI)
    total_tweets = tweets.count()
    processing_counter = total_segs = 0
    logging.info('Going to compute the sentiment of {0:,} tweets'.format(total_tweets))
    for tweet in tweets:
        start_time = time.time()
        processing_counter += 1        
        tweet_id = tweet['id']
        # get text of tweet        
        if 'extended_tweet' in tweet:
            tweet_txt = tweet['extended_tweet']['full_text']
        else:
            tweet_txt = tweet['text']
        tweet_lang = tweet['lang']
        # do preprocessing, remove: hashtags, urls,
        # mentions, reserved words (e.g., RET, FAV),
        # and numbers
        processed_txt = tw_preprocessor.clean(tweet_txt)
        logging.info('[{0}/{1}] Processing tweet:\n{2}'.\
                     format(processing_counter, total_tweets, processed_txt))        
        sentiment_analysis_ret = sa.analyze_sentiment(processed_txt, tweet_lang)        
        logging.info('Sentiment of tweet: {}'.\
                     format(sentiment_analysis_ret['sentiment_score']))
        dbm.update_record({'id': tweet_id}, sentiment_analysis_ret)
        end_time = time.time()
        et_seg = end_time - start_time
        total_segs += et_seg * (total_tweets - processing_counter)
        remaining_time = str(timedelta(seconds=total_segs/processing_counter))
        logging.info('Time remaining to process all tweets: ' \
                     '{} to complete.'.format(remaining_time))
