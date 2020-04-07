import csv
import logging
import pathlib
import os
import preprocessor as tw_preprocessor
import re
import time

from datetime import datetime, timedelta
from utils.language_detector import detect_language
from utils.db_manager import DBManager
from utils.utils import get_tweet_datetime, SPAIN_LANGUAGES, \
        get_covid_keywords, get_spain_places_regex
from utils.sentiment_analyzer import SentimentAnalyzer


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)


# set option of preprocessor
tw_preprocessor.set_options(tw_preprocessor.OPT.URL, 
                            tw_preprocessor.OPT.MENTION, 
                            tw_preprocessor.OPT.HASHTAG,
                            tw_preprocessor.OPT.RESERVED,
                            tw_preprocessor.OPT.NUMBER,
                            tw_preprocessor.OPT.EMOJI)


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


def add_date_time_field_tweet_objs(collection):
    """
    Add date fields to tweet documents
    """
    dbm = DBManager(collection)
    tweets = dbm.search({})
    for tweet in tweets:
        tweet_id = tweet['id']
        logging.info('Generating the datetime of tweet: {}'.format(tweet_id))
        str_tw_dt = tweet['created_at']
        tw_dt, tw_d, tw_t = get_tweet_datetime(str_tw_dt)
        tweet['date_time'] = tw_dt
        tweet['date'] = tw_d
        tweet['time'] = tw_t
        try:
            dbm.update_record({'id': tweet_id}, tweet)
        except:
            pass


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
                

def compute_sentiment_analysis_tweet(tweet, sentiment_analyzer):
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
    sentiment_analysis_ret = sentiment_analyzer.analyze_sentiment(processed_txt, 
                                                                  tweet_lang)
    if sentiment_analysis_ret:
        logging.info('Sentiment of tweet: {}'.\
                     format(sentiment_analysis_ret['sentiment_score']))
    return sentiment_analysis_ret


def prepare_sentiment_obj(sentiment_analysis_ret):
    sentiment_dict = {
        'sentiment': {
            'score': sentiment_analysis_ret['sentiment_score']
        }
    }
    for tool_name, raw_score in sentiment_analysis_ret.items():
        if tool_name != 'sentiment_score':
            sentiment_dict['sentiment'][tool_name] = raw_score
    return sentiment_dict


def compute_sentiment_analysis_tweets(collection, config_fn=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    logging.info('Searching tweets...')
    query = {        
        'sentiment': {'$exists': 0},
        'covid_keywords': {'$exists': 1},
        'lang_esp': {'$exists': 1},
        'place_esp': {'$exists': 1},
    }
    tweets = dbm.search(query)
    sa = SentimentAnalyzer()                       
    total_tweets = tweets.count()
    processing_counter = total_segs = 0
    logging.info('Going to compute the sentiment of {0:,} tweets'.format(total_tweets))
    id_org_processed_tweets = []
    for tweet in tweets:
        tweet_id = tweet['id']
        if tweet_id not in id_org_processed_tweets:
            start_time = time.time()
            processing_counter += 1
            logging.info('[{0}/{1}] Processing tweet:\n{2}'.\
                        format(processing_counter, total_tweets, tweet['text']))
            if 'retweeted_status' not in tweet:        
                tweet_id = tweet['id']
                sentiment_analysis_ret = compute_sentiment_analysis_tweet(tweet, sa)
                if sentiment_analysis_ret:
                    sentiment_dict = prepare_sentiment_obj(sentiment_analysis_ret)
                    dbm.update_record({'id': int(tweet_id)}, sentiment_dict)
                    id_org_processed_tweets.append(tweet_id)
            else:
                logging.info('Found a retweet')
                id_original_tweet = tweet['retweeted_status']['id']
                original_tweet = dbm.find_record({'id': int(id_original_tweet)})
                if 'sentiment' in original_tweet:
                    logging.info('Updating retweet from original tweet')
                    dbm.update_record({'id': int(tweet_id)}, original_tweet['sentiment'])
                else:
                    if 'covid_keywords' in original_tweet and \
                       'lang_esp' in original_tweet and \
                       'place_esp' in original_tweet:
                        sentiment_analysis_ret = compute_sentiment_analysis_tweet(original_tweet, sa)
                        if sentiment_analysis_ret:
                            sentiment_dict = prepare_sentiment_obj(sentiment_analysis_ret)
                            dbm.update_record({'id': int(original_tweet['id'])}, sentiment_dict)
                            id_org_processed_tweets.append(original_tweet['id'])
                            dbm.update_record({'id': int(tweet_id)}, sentiment_dict)
                    else:
                        logging.info('Ignoring retweet. The retweeted tweet is not relevant')
            end_time = time.time()
            total_segs += end_time - start_time
            remaining_secs = (total_segs/processing_counter) * (total_tweets - processing_counter)
            remaining_time = str(timedelta(seconds=remaining_secs))
            logging.info('Time remaining to process all tweets: ' \
                        '{} to complete.'.format(remaining_time))


def identify_duplicates():
    dbm = DBManager(collection='tweets_esp_hpai')
    id_duplicated_tweets = dbm.get_id_duplicated_tweets()
    print(id_duplicated_tweets)


def do_add_covid_keywords_flag(query, dbm):    
    tweets = dbm.search(query)
    total_tweets = tweets.count()    
    for tweet in tweets:
        dbm.update_record({'id': int(tweet['id'])}, {'covid_keywords': 1})
    return total_tweets


def add_covid_keywords_flag(collection, config_fn=None):
    """
    Add field covid_keywords to:
    1. Tweets that contain covid keywords in their hashtags;
    2. Tweets that contain covid keywords in their text (or full-text)
    3. Retweets, whose original tweet contain covid keywords in their text
    4. Quotes, whose original tweet contain covid keywords in their text
    5. Retweet of quotes, whose original tweet contain covid keywords in their
    text.
    All of these situations are reflected in the filter_query dictionary
    """
    start_time = time.time()
    dbm = DBManager(collection=collection, config_fn=config_fn)
    covid_kws = get_covid_keywords()
    covid_kw_regex_objs = []
    for covid_kw in covid_kws:        
        regex_kw = '.*{}.*'.format(covid_kw)
        covid_kw_regex_objs.append(re.compile(regex_kw, re.IGNORECASE))    
    logging.info('Updating tweets that contain covid keywords...')
    filter_query = {
        'covid_keywords': {'$exists': 0},
        '$or': [
            {'entities.hashtags.text': {'$in': covid_kws}},
            {'extended_tweet.full_text': {'$in': covid_kw_regex_objs}},
            {'text': {'$in': covid_kw_regex_objs}},
            {'retweeted_status.extended_tweet.full_text': {'$in': covid_kw_regex_objs}},
            {'retweeted_status.extended_tweet.text': {'$in': covid_kw_regex_objs}},
            {'quoted_status.extended_tweet.full_text': {'$in': covid_kw_regex_objs}},
            {'quoted_status.extended_tweet.text': {'$in': covid_kw_regex_objs}},
            {'retweeted_status.quoted_status.extended_tweet.full_text': 
                {'$in': covid_kw_regex_objs}},
            {'retweeted_status.quoted_status.extended_tweet.text': 
                {'$in': covid_kw_regex_objs}},
        ]
    }
    update_dict = {
        'covid_keywords': 1
    }
    ret_update = dbm.update_record_many(filter_query, update_dict)
    # Print final message
    logging.info('Out of {0:,} tweets matched, {1:,} of them were updated'.\
                 format(ret_update.matched_count, ret_update.modified_count))
    end_time = time.time()
    total_segs = end_time - start_time
    logging.info('Process lasted: {}.'.format(str(timedelta(seconds=total_segs))))


def add_lang_flag(collection, config_fn=None):
    """
    Add field lang_esp for tweets whose language is either
    Spanish, Vasque, Catalan, Gallician, Asturian, or Aragonese
    """
    start_time = time.time()
    dbm = DBManager(collection=collection, config_fn=config_fn)
    filter_query = {
        'lang': {'$in': SPAIN_LANGUAGES}
    }
    update_dict = {
        'lang_esp': 1
    }
    logging.info('Updating tweets written in a spain language {} ...'.\
                 format(SPAIN_LANGUAGES))
    ret_update = dbm.update_record_many(filter_query, update_dict)
    # Print final message
    logging.info('Out of {0:,} tweets matched, {1:,} of them were updated'.\
                 format(ret_update.matched_count, ret_update.modified_count))
    end_time = time.time()
    total_segs = end_time - start_time
    logging.info('Process lasted: {}.'.format(str(timedelta(seconds=total_segs))))


def add_place_flag(collection, config_fn=None):
    start_time = time.time()
    dbm = DBManager(collection=collection, config_fn=config_fn)
    filter_query = {
        '$or': [
            {'place.country': 'Spain'},
            {'user.location': {'$in': get_spain_places_regex()}
            }
        ]
    }
    update_dict = {
        'place_esp': 1
    }
    logging.info('Updating tweets from Spain...')
    ret_update = dbm.update_record_many(filter_query, update_dict)
    # Print final message
    logging.info('Out of {0:,} tweets matched, {1:,} of them were updated'.\
                 format(ret_update.matched_count, ret_update.modified_count))
    end_time = time.time()
    total_segs = end_time - start_time
    logging.info('Process lasted: {}.'.format(str(timedelta(seconds=total_segs))))