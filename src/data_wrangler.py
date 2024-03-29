import csv
import demoji
import json
import logging
import numpy as np
import pathlib
import os
import pandas as pd
import PIL
import preprocessor as tw_preprocessor
import pymongo
import re
import time
import sys

from collections import defaultdict
from datetime import date, datetime, timedelta
from m3inference import M3Twitter
from m3inference.dataset import M3InferenceDataset
from m3inference import consts
from report_generator import pre_process_data
from pymongo.errors import AutoReconnect, ExecutionTimeout, NetworkTimeout
from utils.demographic_detector import DemographicDetector
from utils.embeddings_trainer import EmbeddingsTrainer
from utils.language_detector import detect_language, do_detect_language
from utils.location_detector import LocationDetector
from utils.db_manager import DBManager
from utils.utils import get_tweet_datetime, SPAIN_LANGUAGES, \
        get_covid_keywords, get_spain_places_regex, get_spain_places, \
        calculate_remaining_execution_time, get_config, normalize_text, \
        exists_user, check_user_profile_image
from utils.sentiment_analyzer import SentimentAnalyzer
from torchvision import transforms
from twarc import Twarc
from tqdm import tqdm


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

logging.getLogger('requests').setLevel(logging.CRITICAL)


# set option of preprocessor
tw_preprocessor.set_options(tw_preprocessor.OPT.URL, 
                            tw_preprocessor.OPT.MENTION, 
                            tw_preprocessor.OPT.HASHTAG,
                            tw_preprocessor.OPT.RESERVED,
                            tw_preprocessor.OPT.NUMBER,
                            tw_preprocessor.OPT.EMOJI)


BATCH_SIZE = 5000


def setup_logger(name, log_file, level=logging.INFO):
    handler = logging.FileHandler(log_file)        
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)

    return logger


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


def add_date_time_field_tweet_objs(collection_name, config_fn=None):
    """
    Add date fields to tweet documents
    """
    dbm = DBManager(collection_name, config_fn=config_fn)
    query = {
        'date_time': {'$exists': 0}
    }
    tweets = dbm.search(query)
    total_tweets_to_process = tweets.count()
    total_segs = 0
    tweets_counter = 0
    update_queries = []
    for tweet in tweets:
        start_time = time.time()
        tweets_counter += 1
        tweet_id = tweet['id']
        logging.info('Generating the datetime of tweet: {}'.format(tweet_id))
        str_tw_dt = tweet['created_at']
        tw_dt, tw_d, tw_t = get_tweet_datetime(str_tw_dt)
        tweet_dates = {
            'date_time': tw_dt,
            'date': tw_d,
            'time': tw_t
        }        
        update_queries.append(
            {
                'filter': {'id': int(tweet_id)},
                'new_values': tweet_dates
            }
        )
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        tweets_counter, 
                                                        total_tweets_to_process)
    logging.info('Adding datetime fields to tweets...')
    ret = dbm.bulk_update(update_queries)
    modified_docs = ret.bulk_api_result['nModified']
    logging.info('Added datetime fields to {0:,} tweets'.format(modified_docs))


def check_datasets_intersection():
    dbm = DBManager(config_fn='config_mongo_hpai.json', collection='tweets')
    data_dir = '../data/bsc/'
    remote_total_tweets = dbm.num_records_collection()
    logging.info('Total tweets in remote database: {0:,}'.format(remote_total_tweets))
    local_total_tweets = 0
    total_intersections = 0
    dt_now_str = datetime.today().strftime("%d-%m-%Y")
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


def get_tweet_text(tweet):
    if 'extended_tweet' in tweet:
        tweet_txt = tweet['extended_tweet']['full_text']
    else:
        tweet_txt = tweet['text']
    return tweet_txt


def compute_sentiment_analysis_tweet(tweet, sentiment_analyzer):
    # get text of tweet        
    tweet_txt = get_tweet_text(tweet)
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


def compute_sentiment_analysis_tweets(collection, config_fn=None, 
                                      source_collection=None, date=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    dbm_source = None
    if source_collection:
        dbm_source = DBManager(collection=source_collection, config_fn=config_fn)    
    if date:
        query = {
            'created_at_date': date
        }
    else:
        query = {
            'sentiment': {'$eq': None}
        }
    projection = {
        '_id': 0,
        'id_str': 1,
        'retweeted_status': 1,
        'text': 1,
        'lang': 1,
        'extended_tweet': 1
    }
    logging.info('Retrieving tweets...')
    tweets = list(dbm.find_all(query, projection))
    sa = SentimentAnalyzer()                       
    total_tweets = len(tweets)
    logging.info('Computing the sentiment of {0:,} tweets'.format(total_tweets))
    max_batch = BATCH_SIZE if total_tweets > BATCH_SIZE else total_tweets 
    processing_counter = total_segs = 0
    processed_sentiments = {}
    update_queries = []
    for tweet in tweets:
        start_time = time.time()
        processing_counter += 1
        tweet_id = tweet['id_str']
        source_tweet = None
        if dbm_source:
            source_tweet = dbm_source.find_record({'id': int(tweet_id)})
        if source_tweet and 'sentiment' in source_tweet:
            sentiment_dict = source_tweet['sentiment']
            logging.info('[{0}/{1}] Found tweet in source collection'.\
                format(processing_counter, total_tweets))
        else:
            if tweet_id not in processed_sentiments:                
                logging.info('[{0}/{1}] Computing sentiment of tweet:\n{2}'.\
                            format(processing_counter, total_tweets, tweet['text']))
                if 'retweeted_status' not in tweet:
                    sentiment_analysis_ret = compute_sentiment_analysis_tweet(tweet, sa)
                    sentiment_dict = None
                    if sentiment_analysis_ret:
                        sentiment_dict = prepare_sentiment_obj(sentiment_analysis_ret)
                        processed_sentiments[tweet_id] = sentiment_dict
                else:
                    logging.info('Found a retweet')
                    id_org_tweet = tweet['retweeted_status']['id']                
                    if id_org_tweet not in processed_sentiments:   
                        original_tweet = tweet['retweeted_status']
                        sentiment_analysis_ret = compute_sentiment_analysis_tweet(original_tweet, sa)
                        sentiment_dict = None
                        if sentiment_analysis_ret:
                            sentiment_dict = prepare_sentiment_obj(sentiment_analysis_ret)
                            processed_sentiments[id_org_tweet] = sentiment_dict
                    else:
                        sentiment_dict = processed_sentiments[id_org_tweet]
            else:
                sentiment_dict = processed_sentiments[tweet_id]
        if sentiment_dict:
            update_queries.append(
                {
                    'filter': {'id_str': tweet_id},
                    'new_values': sentiment_dict
                }                        
            )
        if len(update_queries) == max_batch:
            add_fields(dbm, update_queries)
            update_queries = []
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_tweets)            
    if len(update_queries) > 0:
        add_fields(dbm, update_queries)        


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
    covid_kw_regexs = ' '.join(covid_kws)
    logging.info('Updating tweets that contain covid keywords...')
    filter_query = {
        '$and': [
            {'covid_keywords': {'$exists': 0}},
            {'$text': {'$search': covid_kw_regexs}}
        ]
    }
    logging.info('Filter query: {0}'.format(filter_query))
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

def sentiment_evaluation():
    sentiments_voting = {'positivo': 0, 'negativo': 0, 'neutral': 0}
    correct_counter = 0
    total = 600
    with open('../data/bsc/processing_outputs/sentiment_analysis_qualitative_evaluation_2.csv') as csv_file:        
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            true_label = row['clean_nataly'].strip()
            sentiments_voting[row['sentimiento_polyglot'].strip()] += 1
            sentiments_voting[row['sentimiento_affin'].strip()] += 1
            sentiments_voting[row['sentimiento_sentipy'].strip()] += 1
            if sentiments_voting['positivo'] == sentiments_voting['negativo'] == \
               sentiments_voting['neutral']:
                infered_label = row['sentimiento_affin']
            else:
                max_value = sentiments_voting['positivo']
                max_sentiment = 'positivo'
                for sentiment, value in sentiments_voting.items():
                    if sentiment != 'positivo':
                        if value > max_value:
                            max_value = value
                            max_sentiment = sentiment
                infered_label = max_sentiment
            if true_label == infered_label:
                correct_counter += 1
    print('Correct: {0} ({1}%)'.format(correct_counter, correct_counter/total))


def update_sentiment_score_fields(collection, config_fn=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    tweets = dbm.search({'sentiment': {'$exists': 0}})
    for tweet in tweets:
        sentiment_dict = {'score': tweet['score']}
        fields_to_remove = {'score': 1}
        if 'sentiment_score_polyglot' in tweet:
            fields_to_remove.update({'sentiment_score_polyglot': 1})
            sentiment_dict.update({'sentiment_score_polyglot': tweet['sentiment_score_polyglot']})
        if 'sentiment_score_affin' in tweet:
            fields_to_remove.update({'sentiment_score_affin': 1})
            sentiment_dict.update({'sentiment_score_affin': tweet['sentiment_score_affin']})
        if 'sentiment_score_sentipy' in tweet:
            fields_to_remove.update({'sentiment_score_sentipy': 1})
            sentiment_dict.update({'sentiment_score_sentipy': tweet['sentiment_score_sentipy']})
        logging.info('Updating tweet: {}'.format(tweet['id']))
        dbm.update_record({'id': int(tweet['id'])}, {'sentiment': sentiment_dict})
        dbm.remove_field({'id': int(tweet['id'])}, fields_to_remove)


def do_drop_collection(collection, config_fn=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    return dbm.drop_collection()


def test_vader_sa():
    file_path = '../data/bsc/processing_outputs/sentiment_analysis_sample_scores.csv'
    sa = SentimentAnalyzer()
    dbm = DBManager('tweets_esp')
    tweets_analyzed = []

    tw_preprocessor.set_options(tw_preprocessor.OPT.URL, 
                                tw_preprocessor.OPT.MENTION, 
                                tw_preprocessor.OPT.HASHTAG,
                                tw_preprocessor.OPT.RESERVED)

    with open(file_path, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            print(row['id'])
            tweet = dbm.find_record({'id': int(row['id'])})
            text = tweet['extended_tweet']['full_text']
            text_lang = tweet['lang']
            logging.info('Analyzing tweets {}'.format(text))
            # apply text preprocessor
            clean_text = tw_preprocessor.clean(text)
            # find emojis
            emojis = demoji.findall(clean_text)
            # remove emojis if they exist
            if emojis:
                clean_text = demoji.replace(clean_text).replace('\u200d️','').strip()
            # translate text to english
            logging.info('Text to translate {}'.format(clean_text))
            translated_text = sa.translate_text(clean_text, source_lang=text_lang)
            # add emojis
            if emojis:
                for emoji, _ in emojis.items():
                    translated_text += ' ' + emoji
            # compute sentiment
            sentiment_score = sa.analyze_sentiment_vader(translated_text)
            tweets_analyzed.append(
                {
                    'id': row['id'],
                    'text': text,
                    'lang': text_lang,
                    'score_vader': sentiment_score
                }
            )
    
    output_file = '../data/bsc/processing_outputs/sentiment_analysis_sample_vader.csv'
    logging.info('Saving tweets to the CSV {}'.format(output_file))
    with open(output_file, 'w') as csv_file:
        headers = tweets_analyzed[0].keys()
        csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
        csv_writer.writeheader()
        for tweet_analyzed in tweets_analyzed:
            csv_writer.writerow(tweet_analyzed)


def add_fields(dbm, update_queries):
    logging.info('Adding fields to tweets...')
    ret = dbm.bulk_update(update_queries)
    modified_tweets = ret.bulk_api_result['nModified']
    logging.info('Added fields to {0:,} tweets'.format(modified_tweets))


def do_add_language_flag(collection, config_fn=None, tweets_date=None, 
                         source_collection=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    dbm_source = None
    if source_collection:
        dbm_source = DBManager(collection=source_collection, config_fn=config_fn)
    query = {
        'lang': 'es',
        'lang_detection': {'$eq': None}
    }
    if tweets_date:
        query['created_at_date'] = tweets_date
    projection = {
        '_id': 0,
        'id_str': 1,
        'retweeted_status': 1,
        'text': 1,
        'lang': 1,
        'extended_tweet': 1
    }
    tweets_es = dbm.find_all(query, projection)
    total_tweets = tweets_es.count()
    logging.info('Processing language of {0:,} tweets'.format(total_tweets))
    max_batch = BATCH_SIZE if total_tweets > BATCH_SIZE else total_tweets 
    processing_counter = total_segs = 0    
    processed_tweets = {}
    update_queries = []
    spain_languages = ['ca', 'eu', 'gl']
    for tweet in tweets_es: 
        tweet_id = tweet['id_str']
        start_time = time.time()
        processing_counter += 1
        source_tweet = None
        if dbm_source:
            source_tweet = dbm_source.find_record({'id': int(tweet_id)})
        if source_tweet and 'lang_detection' in source_tweet and \
           'lang_twitter' in source_tweet:
            new_values = {
                'lang_detection': source_tweet['lang_detection'],
                'lang': source_tweet['lang'],
                'lang_twitter': source_tweet['lang_twitter']
            }            
            logging.info('[{0}/{1}] Found tweet in source collection'.format(processing_counter, total_tweets))
        else:
            if tweet_id not in processed_tweets:
                logging.info('[{0}/{1}] Detecting language of tweet:\n{2}'.\
                            format(processing_counter, total_tweets, tweet['text']))
                if 'retweeted_status' not in tweet:
                    tweet_lang = tweet['lang']
                    tweet_txt = tw_preprocessor.clean(get_tweet_text(tweet))
                    lang_dict = detect_language(tweet_txt)
                    if lang_dict: processed_tweets[tweet_id] = lang_dict
                else:
                    logging.info('Found a retweet')
                    original_tweet = tweet['retweeted_status']
                    id_org_tweet = original_tweet['id']                
                    tweet_lang = original_tweet['lang']
                    if id_org_tweet not in processed_tweets:
                        tweet_txt = tw_preprocessor.clean(get_tweet_text(original_tweet))
                        lang_dict = detect_language(tweet_txt)
                        if lang_dict: processed_tweets[id_org_tweet] = lang_dict
                    else:
                        lang_dict = processed_tweets[id_org_tweet]
            else:
                lang_dict = processed_tweets[tweet_id]
            new_values = {
                'lang_detection': lang_dict
            }
            if lang_dict and lang_dict['pref_lang'] != 'undefined' and \
            lang_dict['pref_lang'] != tweet_lang and \
            lang_dict['pref_lang'].find('_') == -1 and \
            lang_dict['pref_lang'] in spain_languages:
                new_values.update(
                    {
                        'lang': lang_dict['pref_lang'],
                        'lang_twitter': tweet_lang
                    }
                )
        update_queries.append(
            {
                'filter': {'id_str': tweet_id},
                'new_values': new_values
            }                        
        )
        if len(update_queries) >= max_batch:
            add_fields(dbm, update_queries)
            update_queries = []
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_tweets)                    
    if len(update_queries) > 0:
        add_fields(dbm, update_queries)


def do_add_query_version_flag(collection, config_fn=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    file_query_versions = str(pathlib.Path(__file__).parents[1].joinpath('data','query_versions.csv'))    
    with open(file_query_versions, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            query_version = row['version']
            start_date = row['start_date']
            end_date = row['end_date']
            query = {
                'created_at_date': {'$gte': start_date},
                'query_version': {'$exists': 0}
            }
            if end_date:
                query['created_at_date'].update({'$lte': end_date})
            flag_to_add = {
                'query_version': query_version
            }
            logging.info('Updating records with query {}'.format(query))
            ret_update = dbm.update_record_many(query, flag_to_add)
            logging.info('Out of {0:,} tweets matched, {1:,} of them were updated'.\
                         format(ret_update.matched_count, ret_update.modified_count))


def clean_location(location):
    location = location.lower().strip().replace('.','').\
                    replace('(','').replace(')','').replace('d’','').\
                    replace('L’','').replace(']','').replace('[','').\
                    replace('#','').replace('"','')
    return normalize_text(location)


def identify_location(locations, places_esp, n_places_esp, cities, provinces, 
                      ccaas, ccaa_province):
    found_place, found_esp = False, False
    for location in locations:
        if not location:
            continue
        location = clean_location(location)
        if location in cities:
            place_idx = n_places_esp[n_places_esp['ciudad']==location].index[0]
            found_place=True
        elif location in provinces:
            place_idx = n_places_esp[n_places_esp['provincia']==location].index[0]
            found_place=True
        elif location in ccaas:
            place_idx = n_places_esp[n_places_esp['comunidad autonoma']==location].index[0]
            found_place=True
        if found_place:
            ccaa_province['comunidad_autonoma'] = places_esp.loc[place_idx, 'comunidad autonoma']
            ccaa_province['provincia'] = places_esp.loc[place_idx, 'provincia'] 
            break
        else:
            if not found_esp:
                found_esp = location in ['espana', 'spain', 'espanya', 'es']
    if not found_place and found_esp:
        ccaa_province['comunidad_autonoma'] = ccaa_province['provincia'] = 'España'


def identify_unknown_locations(locations, places_esp, n_places_esp, cities, 
                               provinces, ccaas, ccaa_province ):
    found_place = False
    for location in locations:
        if not location:
            continue
        location = clean_location(location)
        for city in cities:
            for sub_city in city.split():
                if location == sub_city:
                    place_idx = n_places_esp[n_places_esp['ciudad']==city].index[0]
                    found_place = True
                    break
        if not found_place:
            for province in provinces:
                for sub_province in province.split():
                    if location == sub_province:
                        place_idx = n_places_esp[n_places_esp['provincia']==province].index[0]
                        found_place = True
                        break
        if not found_place:
            for ccaa in ccaas:
                for sub_ccaa in ccaa.split():
                    if location == sub_ccaa:
                        place_idx = n_places_esp[n_places_esp['comunidad autonoma']==ccaa].index[0]
                        found_place = True
                        break
        if found_place:
            ccaa_province['comunidad_autonoma'] = places_esp.loc[place_idx,'comunidad autonoma']
            ccaa_province['provincia'] = places_esp.loc[place_idx, 'provincia']


def add_esp_location_flags(collection, config_fn, doc_type='tweet'):
    """
    doc_type: can be tweet or user
    """

    current_path = pathlib.Path(__file__).parent.resolve()
    places_esp_fn = os.path.join(current_path, '..', 'data', 'places_spain.json')
    detector = LocationDetector(places_esp_fn, flag_in_location=True, 
                                demonym_in_description=True,
                                language_of_description=True)
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {        
        '$or': [
            {'comunidad_autonoma': {'$eq': None}},
            {'comunidad_autonoma': 'no determinado'}
        ]
    }
    if doc_type == 'tweet':
        projection = {
            '_id':0,
            'id_str':1,
            'user.screen_name':1,
            'user.description': 1,
            'user.location':1,
            'place.full_name': 1
        }
    else:
        projection = {
            '_id':0,
            'id_str':1,
            'screen_name':1,
            'description': 1,
            'location':1
        }
    logging.info('Getting documents...')
    docs = list(dbm.find_all(query, projection))
    total_docs = len(docs)
    logging.info('Processing locations of {0:,} documents'.format(total_docs))
    update_queries = []
    max_batch = BATCH_SIZE if total_docs > BATCH_SIZE else total_docs
    processing_counter = total_segs = 0
    for doc in docs:
        start_time = time.time()
        doc_id = doc['id_str']
        processing_counter += 1
        logging.info('Processing document {}'.format(doc['id_str']))
        user_location = ''
        if 'user' in doc: 
            if doc['user']['location'] != '':
                user_location = doc['user']['location']
            elif 'place' in doc:
                user_location = doc['place']['full_name']                
            user_description = doc['user']['description']
        else:
            user_location = doc['location']
            user_description = doc['description']
        location, method = detector.identify_location(user_location, user_description)
        if location == 'unknown':
            location = 'no determinado'
            method = ''
        location_dict = {
            'comunidad_autonoma': location,
            'identification_method': method
        }
        update_queries.append(
            {
                'filter': {'id_str': str(doc_id)},
                'new_values': location_dict
            }                        
        )
        if len(update_queries) == max_batch:
            add_fields(dbm, update_queries)
            update_queries = []
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_docs)
    if len(update_queries) > 0:
        add_fields(dbm, update_queries)


def get_twm_obj():
    current_path = pathlib.Path(__file__).parent.resolve()
    config = get_config(os.path.join(current_path, 'config.json'))
    twm = Twarc(config['twitter_api']['consumer_key'], 
                config['twitter_api']['consumer_secret'],
                config['twitter_api']['access_token'],
                config['twitter_api']['access_token_secret'])
    return twm


def update_metric_tweets(collection, config_fn=None, source_collection=None,
                         date=None):
    current_path = pathlib.Path(__file__).parent.resolve()
    logging_file = os.path.join(current_path, 'tw_coronavirus.log')    
    logger = setup_logger('logger', logging_file)
    twm = get_twm_obj()
    dbm = DBManager(collection=collection, config_fn=config_fn)
    dbm_source = None
    if source_collection:
        dbm_source = DBManager(collection=source_collection, config_fn=config_fn)
    current_date = datetime.today()
    current_date_str = current_date.strftime('%Y-%m-%d')
    if date:
        query = {
            'created_at_date': date
        }
    else:
        query = {
            '$or': [
                {'last_metric_update_date': {'$eq': None}},
                {'next_metric_update_date': current_date_str}            
            ]                    
        }
    projection = {
        '_id':0,
        'id_str':1,
        'created_at_date':1,
        'retweeted_status.id_str': 1
    }
    #PAGE_SIZE = 500000
    #page_num = 0
    #records_to_read = True
    processing_counter = total_segs = 0
    #while records_to_read:
    #    page_num += 1
    #    pagination = {'page_num': page_num, 'page_size': PAGE_SIZE}
    logger.info('Retrieving tweets...')
    tweet_objs = dbm.find_all(query=query, projection=projection)
    tweets = [tweet_obj for tweet_obj in tweet_objs]
    total_tweets = len(tweets)
    logger.info('Found {:,} tweets'.format(total_tweets))
#    if total_tweets == 0:
#        break
    max_batch = BATCH_SIZE if total_tweets > BATCH_SIZE else total_tweets
    processing_counter = total_segs = 0
    tweet_ids, rts = [], []
    org_tweets = {}
    update_queries = []
    # processing tweets
    logger.info('Processing original tweets...')
    for tweet in tweets:
        start_time = time.time()
        processing_counter += 1
        source_tweet = None
        if dbm_source:
            source_tweet = dbm_source.find_record({'id_str': tweet['id_str']})
        if source_tweet and 'last_metric_update_date' in source_tweet and \
            'next_metric_update_date' in source_tweet:
            new_values = {
                'retweet_count': source_tweet['retweet_count'],
                'favorite_count': source_tweet['favorite_count'],
                'last_metric_update_date': source_tweet['last_metric_update_date'],
                'next_metric_update_date': source_tweet['next_metric_update_date']
            }
            update_queries.append(
                {
                    'filter': {'id_str': tweet['id_str']},
                    'new_values': new_values
                }                        
            )
            logger.info('[{0}/{1}] Found tweet in source collection'.format(processing_counter, total_tweets))
            continue
        if 'retweeted_status' not in tweet.keys():
            tweet_ids.append(tweet['id_str'])
        else:
            rts.append({
                'id_str': tweet['id_str'],
                'parent_id': tweet['retweeted_status']['id_str']
            })
        if len(tweet_ids) == max_batch or processing_counter == (total_tweets-1):
            logger.info('Hydratating tweets...')
            update_queries = []
            tweet_date = datetime.strptime(tweet['created_at_date'], '%Y-%m-%d')
            diff_date = current_date - tweet_date
            next_update_date = current_date + timedelta(days=diff_date.days)
            next_update_date_str = next_update_date.strftime('%Y-%m-%d')
            hydrated_tweet_ids = []
            for tweet_obj in twm.hydrate(tweet_ids):
                new_values = {
                    'retweet_count': tweet_obj['retweet_count'],
                    'favorite_count': tweet_obj['favorite_count'],
                    'last_metric_update_date': current_date_str,
                    'next_metric_update_date': next_update_date_str
                }
                org_tweets[tweet_obj['id_str']] = new_values
                update_queries.append(
                    {
                        'filter': {'id_str': tweet_obj['id_str']},
                        'new_values': new_values
                    }                        
                )
                hydrated_tweet_ids.append(tweet_obj['id_str'])
            miss_ids = set(tweet_ids) - set(hydrated_tweet_ids)
            logging.info('Out of the {} tweets searched to be hydrated, {} '\
                        'do not exist anymore'.format(len(tweet_ids),len(miss_ids)))
            for miss_id in miss_ids:
                new_values = {
                    'last_metric_update_date': current_date_str,
                    'next_metric_update_date': '2080-01-01'
                }
                org_tweets[miss_id] = new_values
                update_queries.append(
                    {
                        'filter': {'id_str': miss_id},
                        'new_values': new_values
                    }                        
                )
            if len(update_queries) > 0:
                add_fields(dbm, update_queries)
            tweet_ids = []
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_tweets)
    if len(update_queries) > 0:
        add_fields(dbm, update_queries)
    # processing rts
    logger.info('Processing retweets...')
    update_queries = []
    processing_counter = total_segs = 0
    total_tweets = len(rts)
    for rt in rts:        
        if rt['parent_id'] in org_tweets:
            start_time = time.time()
            processing_counter += 1
            update_queries.append(
                {
                    'filter': {'id_str': rt['id_str']},
                    'new_values': org_tweets[rt['parent_id']]
                }                        
            )
            if len(update_queries) == max_batch:
                add_fields(dbm, update_queries)
                update_queries = []
            total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                            processing_counter, 
                                                            total_tweets)
    if len(update_queries) > 0:
        add_fields(dbm, update_queries)


def do_add_complete_text_flag(collection, config_fn):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {
        'complete_text': {'$eq': None}
    }
    projection = {
        '_id': 0,
        'id_str': 1,
        'text': 1,
        'extended_tweet.full_text': 1,
        'retweeted_status': 1
    }
    logging.info('Finding tweets...')
    tweets = dbm.find_all(query, projection)
    total_tweets = tweets.count()
    logging.info('Found {:,} tweets'.format(total_tweets))
    max_batch = BATCH_SIZE if total_tweets > BATCH_SIZE else total_tweets
    update_queries = []
    processing_counter = total_segs = 0
    for tweet in tweets:
        start_time = time.time()
        processing_counter += 1
        org_tweet = tweet if 'retweeted_status' not in tweet else tweet['retweeted_status']
        complete_text = get_tweet_text(org_tweet)
        update_queries.append({
            'filter': {'id_str': tweet['id_str']},
            'new_values': {'complete_text': complete_text}
        })
        if len(update_queries) == max_batch:
            add_fields(dbm, update_queries)
            update_queries = []
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_tweets)
    if len(update_queries) > 0:
        add_fields(dbm, update_queries)


def get_tweet_type(tweet):
    if 'retweeted_status' in tweet:
        tweet_type = 'retweet'
    elif 'is_quote_status' in tweet and tweet['is_quote_status']:
        tweet_type = 'quote'
    elif 'in_reply_to_status_id_str' in tweet and tweet['in_reply_to_status_id_str']:
        tweet_type = 'reply'
    else:
        tweet_type = 'original'
    return tweet_type


def do_add_tweet_type_flag(collection, config_fn):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {
        'type': {'$eq': None}
    }
    projection = {
        '_id': 0,
        'id_str': 1,
        'retweeted_status': 1,
        'is_quote_status': 1,
        'in_reply_to_status_id_str': 1
    }
    logging.info('Finding tweets...')
    tweets = dbm.find_all(query, projection)
    total_tweets = tweets.count()
    logging.info('Found {:,} tweets'.format(total_tweets))
    max_batch = BATCH_SIZE if total_tweets > BATCH_SIZE else total_tweets
    update_queries = []
    processing_counter = total_segs = 0
    for tweet in tweets:
        start_time = time.time()
        processing_counter += 1
        tweet_type = get_tweet_type(tweet)        
        update_queries.append({
            'filter': {'id_str': tweet['id_str']},
            'new_values': {'type': tweet_type}
        })
        if len(update_queries) == max_batch:
            add_fields(dbm, update_queries)
            update_queries = []
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_tweets)
    if len(update_queries) > 0:
        add_fields(dbm, update_queries)


def do_update_user_status(collection, config_fn=None, log_fn=None):
    current_path = pathlib.Path(__file__).resolve()
    project_dir = current_path.parents[1]
    if log_fn:
        logging_file = os.path.join(current_path, log_fn)
        user_logger = setup_logger('user_logger', logging_file)
    else:
        user_logger = logging
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {
        'predicted': {'$eq': None}
    }
    projection = {
        '_id': 0,
        'id': 1,
        'screen_name': 1,
        'img_path': 1,
        'prediction': 1
    }
    user_logger.info('Retrieving users...')
    users = list(dbm.find_all(query, projection))
    total_users = len(users)
    user_logger.info('Found {:,} users'.format(total_users))
    processing_counter = total_segs = 0
    max_batch = BATCH_SIZE if total_users > BATCH_SIZE else total_users
    users_to_update = []
    for user in users:
        if 'img_path' not in user:
            continue
        start_time = time.time()
        processing_counter += 1
        user_logger.info('Updating user: {}'.format(user['screen_name']))
        if 'prediction' in user:
            users_to_update.append({
                'filter': {'id': int(user['id'])},
                'new_values': {'exists': 1}
            })
        else:
            img_path = user['img_path']
            img_path_to_save = user['img_path']
            if img_path == '[no_img]':
                users_to_update.append({
                    'filter': {'id': int(user['id'])},
                    'new_values': {'exists': 0}
                })
            else:
                if 'user_pics' in img_path:
                    if 'tw_coronavirus' not in img_path:
                        img_path = os.path.join(project_dir, user['img_path'])
                    else:
                        img_path_to_save = '/'.join(img_path.split('/')[-2:])
                    if os.path.exists(img_path):
                        try:
                            check_user_profile_image(img_path)                            
                            users_to_update.append({
                                'filter': {'id': int(user['id'])},
                                'new_values': {'exists': 1, 'img_path': img_path_to_save}
                            })
                        except:
                            users_to_update.append({
                                'filter': {'id': int(user['id'])},
                                'new_values': {'exists': 2}
                            })
                    else:
                        users_to_update.append({
                            'filter': {'id': int(user['id'])},
                            'new_values': {'exists': 0}
                        })
                else:
                    users_to_update.append({
                        'filter': {'id': int(user['id'])},
                        'new_values': {'exists': 2}  # 2 means, the user exists but his/her picture could not be downloaded correctly
                    })
        if len(users_to_update) >= max_batch:            
            add_fields(dbm, users_to_update)
            users_to_update = []        
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_users)
    if len(users_to_update) >= 0:
        add_fields(dbm, users_to_update)


def process_user_batch(users_batch):
    processed_records = []
    for _, user_dict in users_batch.items():
        user_dict['exists'] = 1        
        processed_records.append(user_dict)
    return processed_records


def process_user(user, tweet):
    if 'tweet_ids' in user and tweet['id_str'] in set(user['tweet_ids']):
        return None
    
    if 'tweet_ids' not in user:
        user['tweet_ids'] = [tweet['id_str']]
        user['tweet_dates'] = [tweet['created_at_date']]
    else:
        user['tweet_ids'].append(tweet['id_str'])
        user['tweet_dates'].append(tweet['created_at_date'])
    
    user['exists'] = 1
    user['total_tweets'] += 1
    if tweet['comunidad_autonoma'] != 'desconocido':
        user['comunidad_autonoma'] = tweet['comunidad_autonoma']
    if tweet['provincia'] != 'desconocido':
        user['provincia'] = tweet['provincia']    
    tweet_type = get_tweet_type(tweet)
    if tweet_type == 'retweet':
        user['retweets'] += 1
    elif tweet_type == 'reply':
        user['replies'] += 1
    elif tweet_type == 'quote':
        user['quotes'] += 1
    else:
        user['originals'] += 1    
    return user


def do_update_users_collection(collection, user_collection=None, config_fn=None, 
                               log_fn=None):        
    if log_fn:
        current_path = pathlib.Path(__file__).parent.resolve()
        logging_file = os.path.join(current_path, log_fn)
        user_logger = setup_logger('user_logger', logging_file)
    else:
        user_logger = logging
    if not user_collection:
        user_collection='users'
    dbm = DBManager(collection=collection, config_fn=config_fn)
    dbm_users = DBManager(collection=user_collection, config_fn=config_fn)
    query = {
        'processed_user': {'$eq': None}
    }
    projection = {
        '_id': 0,
        'id_str': 1,
        'created_at_date': 1,
        'user': 1,
        'retweeted_status.id': 1,
        'is_quote_status': 1,
        'in_reply_to_status_id_str': 1,
        'comunidad_autonoma': 1,
        'provincia': 1
    }
    PAGE_SIZE = 100000
    page_num = 0
    records_to_read = True
    processing_counter = total_segs = 0
    while records_to_read:
        page_num += 1
        pagination = {'page_num': page_num, 'page_size': PAGE_SIZE}
        user_logger.info('Retrieving tweets...')
        tweets = list(dbm.find_all(query, projection, pagination=pagination))
        user_logger.info('Fetched tweets, now saving them in a list...')
        total_tweets = len(tweets)
        user_logger.info('Found {:,} tweets'.format(total_tweets))
        if total_tweets == 0:
            break
        max_batch = BATCH_SIZE if total_tweets > BATCH_SIZE else total_tweets
        user_update_queries, tweet_update_queries = [], []
        users_to_insert, users_to_update = {}, {}
        # Iterate over page
        for tweet in tweets:
            start_time = time.time()
            processing_counter += 1
            if 'comunidad_autonoma' not in tweet:
                user_logger.info('The field comunidad_autonoma does not exist in the tweet, ignoring...')
                continue
            user = tweet['user']
            user_obj = dbm_users.find_record({'id_str': user['id_str']})
            if user_obj:
                # it the user exists in the database, she might exists already
                # in the batch or not
                if user['id_str'] not in users_to_update:
                    user_to_update = process_user(user_obj, tweet)    
                else:
                    user_to_update = process_user(
                        users_to_update[user['id_str']], tweet)
                if user_to_update:
                    users_to_update[user['id_str']] = user_to_update            
                    user_logger.info('Updating the user {}'.format(user['screen_name']))
            else:
                # it the users does not exists in the database, she might exists already
                # in the batch or not
                if user['id_str'] not in users_to_insert:                
                    new_fields = {
                        'exists': 0,
                        'total_tweets': 0,
                        'retweets': 0,
                        'replies': 0,
                        'quotes': 0,
                        'originals': 0,
                        'comunidad_autonoma': tweet['comunidad_autonoma'],
                        'provincia': tweet['provincia']
                    }
                    user.update(new_fields)
                    user_to_insert = process_user(user, tweet)                  
                else:
                    user_to_insert = process_user(
                        users_to_insert[user['id_str']], tweet)
                if user_to_insert:               
                    users_to_insert[user['id_str']] = user_to_insert           
                    user_logger.info('Adding the user {}'.format(user['screen_name']))
            tweet_update_queries.append({
                'filter': {'id_str': tweet['id_str']},
                'new_values': {'processed_user': 1}
            })
            if len(users_to_insert) >= max_batch:
                logging.info('Inserting {} users'.format(len(users_to_insert)))                
                dbm_users.insert_many(process_user_batch(users_to_insert))
                users_to_insert = {}
            if len(users_to_update) >= max_batch:
                logging.info('Updating {} users'.format(len(users_to_update)))
                processed_users = process_user_batch(users_to_update)
                for processed_user in processed_users:
                    user_update_queries.append({
                        'filter': {'id_str': processed_user['id_str']},
                        'new_values': processed_user
                    })
                add_fields(dbm_users, user_update_queries)
                users_to_update = {}
            if len(tweet_update_queries) >= max_batch:
                logging.info('Updating {} tweets'.format(len(tweet_update_queries)))
                add_fields(dbm, tweet_update_queries)
                tweet_update_queries = []
            total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                            processing_counter, 
                                                            total_tweets)
            user_logger.info('Total users to insert: {0:,} - Total users to update: '\
                            '{1:,} - Total tweets to update: {2:,}'.\
                            format(len(users_to_insert), len(users_to_update), len(tweet_update_queries)))
        if len(users_to_insert) > 0:
            user_logger.info('Inserting {} users'.format(len(users_to_insert)))
            dbm_users.insert_many(process_user_batch(users_to_insert))
        if len(users_to_update) > 0:
            user_logger.info('Updating {} users'.format(len(users_to_update)))
            processed_users = process_user_batch(users_to_update)
            for processed_user in processed_users:
                user_update_queries.append({
                    'filter': {'id_str': processed_user['id_str']},
                    'new_values': processed_user
                })
            add_fields(dbm_users, user_update_queries)
        if len(tweet_update_queries) > 0:
            user_logger.info('Updating {} tweets'.format(len(tweet_update_queries)))
            add_fields(dbm, tweet_update_queries)


def do_augment_user_data(collection, config_fn=None, log_fn=None):
    current_path = pathlib.Path(__file__).resolve()
    project_dir = current_path.parents[1]
    user_pics_dir = 'user_pics'
    user_pics_path = os.path.join(project_dir, user_pics_dir)
    if not os.path.exists(user_pics_path):
        os.mkdir(user_pics_path)
    m3twitter = M3Twitter(cache_dir=user_pics_path)
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {
        'img_path': {'$eq': None}
    }
    projection = {
        '_id': 0,
        'id': 1,
        'id_str': 1,
        'name': 1,
        'screen_name': 1,
        'description': 1,
        'default_profile_image': 1,
        'profile_image_url_https': 1,
    }
    logging.info('Retriving users...')
    users = list(dbm.find_all(query, projection))
    total_users = len(users)
    logging.info('Fetched {} users'.format(total_users))
    processing_counter = total_segs = 0
    max_batch = BATCH_SIZE if total_users > BATCH_SIZE else total_users
    users_to_update = []
    for user in users:
        start_time = time.time()
        processing_counter += 1
        fields_to_update = {}
        try:
            logging.info('Augmenting data of user {}'.format(user['screen_name']))
            augmented_user = m3twitter.transform_jsonl_object(user)
            fields_to_update['img_path'] = '/'.join(augmented_user['img_path'].split('/')[-2:])
            if augmented_user['lang'] is None:
                if 'lang_detected' in user:                    
                    fields_to_update['lang'] = user['lang_detected']
                else:
                    fields_to_update['lang'] = 'un'
            else:
                fields_to_update['lang'] = augmented_user['lang']
            fields_to_update['exists'] = 1
        except:
            logging.info('Could not augment data of user {}'.format(user['screen_name']))
            fields_to_update['img_path'] = '[no_img]'
            fields_to_update['exists'] = 0
        users_to_update.append(
            {
                'filter': {'id': int(user['id'])},
                'new_values': fields_to_update
            }            
        )
        if len(users_to_update) >= max_batch:
            add_fields(dbm, users_to_update)
            users_to_update = []
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_users)
    if len(users_to_update) > 0:
        add_fields(dbm, users_to_update)


def predict_demographics(users_to_predict, demog_detector, dbm):
    users_to_update = []
    try:
        predictions = demog_detector.infer(users_to_predict)        
        users_to_update = []
        predicted_user_ids = []
        for prediction in predictions:
            user_id = prediction['id']
            predicted_user_ids.append(user_id)
            del prediction['id']
            prediction['prediction'] = 'succeded'
            users_to_update.append(
                {
                    'filter': {'id': int(user_id)},
                    'new_values': prediction
                }
            )    
        add_fields(dbm, users_to_update)
    except:
        pass # TODO: Take action when prediction fails


def compute_user_demographics(collection, config_fn=None):
    current_path = pathlib.Path(__file__).resolve()
    project_dir = current_path.parents[1]
    user_pics_dir = 'user_pics'
    user_pics_path = os.path.join(project_dir, user_pics_dir)
    demog_detector = DemographicDetector(user_pics_path)
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {
        'exists': 1              
    }
    projection = {
        '_id': 0,
        'id': 1,
        'id_str': 1,
        'name': 1,
        'screen_name': 1,
        'description': 1,
        'lang': 1,
        'img_path': 1,
    }
    logging.info('Retriving users...')
    users = list(dbm.find_all(query, projection))
    total_users = len(users)
    logging.info('Fetched {} users'.format(total_users))
    processing_counter = total_segs = 0
    max_batch = BATCH_SIZE if total_users > BATCH_SIZE else total_users
    users_to_predict = []
    users_no_prediction = []
    for user in users:
        if 'img_path' not in user:
            continue
        start_time = time.time()
        processing_counter += 1
        logging.info('Collecting user {}'.format(user['screen_name']))
        img_path = user['img_path']
        if 'tw_coronavirus' not in user['img_path']:
            img_path = os.path.join(project_dir, user['img_path'])
        if os.path.exists(img_path):             
            users_to_predict.append(
                {
                    'id': user['id_str'],
                    'name': user['name'],
                    'screen_name': user['screen_name'],
                    'description': user['description'],
                    'lang': user['lang'],
                    'img_path': img_path
                }
            )
        else:
            logging.info('User without profile pic. Imposible to infer her demographic characteristics')
            users_no_prediction.append(
                {
                    'filter': {'id': user['id']},
                    'new_values': {
                        'prediction': 'failed', 
                        'prediction_error': 'image_missing'
                    }
                }
            )
        if len(users_to_predict) >= max_batch:
            logging.info('Doing predictions...')
            predict_demographics(users_to_predict, demog_detector, dbm)
            users_to_predict = []
        if len(users_no_prediction) >= max_batch:
            logging.info('Updating users without profile pic')
            add_fields(dbm, users_no_prediction)
            users_no_prediction = []
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_users)
    if len(users_to_predict) > 0:
        logging.info('Doing final predictions...')
        predict_demographics(users_to_predict, demog_detector, dbm)
    if len(users_no_prediction) > 0:
        logging.info('Updating users without profile pic')
        add_fields(dbm, users_no_prediction)


def compute_user_demographics_from_file(input_file, output_filename=None):
    current_path = pathlib.Path(__file__).resolve()
    project_dir = current_path.parents[1]
    user_pics_path = os.path.join(project_dir, 'user_pics')
    demog_detector = DemographicDetector(user_pics_path)
    user_objs = []
    with open(input_file) as json_file:
        json_lines = json_file.readlines()
        for json_line in json_lines:
            user_obj = json.loads(json_line)
            user_obj['img_path'] = os.path.join(project_dir, user_obj['img_path'])
            if os.path.exists(user_obj['img_path']):
                user_objs.append(user_obj)
            else:
                logging.info('Ignoring {0}, his/her profile pic {1} does not exists'.format(user_obj['screen_name'], user_obj['img_path']))
    predictions = demog_detector.infer(user_objs)
    demog_detector.save_predictions(predictions, output_filename)


def check_user_pictures_from_file(input_file):
    current_path = pathlib.Path(__file__).resolve()
    project_dir = current_path.parents[1]
    tensor_trans = transforms.ToTensor()
    user_objs = []
    with open(input_file) as json_file:
        json_lines = json_file.readlines()
        for json_line in json_lines:
            user_obj = json.loads(json_line)
            user_obj['img_path'] = os.path.join(project_dir, user_obj['img_path'])
            if os.path.exists(user_obj['img_path']):
                user_objs.append(user_obj)
    dataset = M3InferenceDataset(user_objs)
    sizes = defaultdict(list)
    total_processed = 0
    for i in range(len(dataset.data)):        
        try:
            data = dataset.data[i]
            p_data = dataset._preprocess_data(data)
            total_processed += 1
            fig = p_data[-1]
            fig_size = fig.size()
            if fig_size[0] == 3 or fig_size[1] == 224 or fig_size[2] == 224:
                sizes['3x224x224'].append(data[-1])
            elif fig_size[0] == 1 or fig_size[1] == 224 or fig_size[2] == 224:
                sizes['1x224x224'].append(data[-1])
            else:
                sizes['other'].append(data[-1])
        except Exception as e:
            print('Error {}'.format(e))
            print('{}'.format(dataset.data[i]))
        
    print('Total processed: {}'.format(total_processed))
    for k, v in sizes.items():
        print('{0}: {1}'.format(k, len(v)))            
        

def check_user_pictures(collection, config_fn=None):
    current_path = pathlib.Path(__file__).resolve()
    project_dir = current_path.parents[1]
    user_pics_dir = 'user_pics'
    user_pics_path = os.path.join(project_dir, user_pics_dir)
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {
        '$and': [
            {'img_path': {'$ne': None}},
            {'img_path': {'$ne': '[no_img]'}}
        ]                
    }
    projection = {
        '_id': 0,
        'id_str': 1,        
        'img_path': 1
    }
    print('Retriving users...')
    users = list(dbm.find_all(query, projection))
    total_users = len(users)
    print('Fetched {0:,} users'.format(total_users))
    CORRECT_IMG_SIZE = (224, 224)
    print('Analyzing picture of users, please wait...')
    ok_users, problem_users = 0, 0
    output_file_name = os.path.join(project_dir, 'data', 'users_pic.csv')
    with tqdm(total=total_users, file=sys.stdout) as pbar:
        with open(output_file_name, 'w') as output_file:
            csv_writer = csv.DictWriter(output_file, fieldnames=['id','problem','problem_info'])
            csv_writer.writeheader()            
            for user in users:
                if 'prediction' not in user:            
                    img_path = user['img_path']
                    if 'user_pics' in img_path:
                        if 'tw_coronavirus' not in img_path:
                            img_path = os.path.join(project_dir, user['img_path'])
                        if os.path.exists(img_path):
                            try:
                                image = PIL.Image.open(img_path)
                                img_size = image.size
                                if img_size == CORRECT_IMG_SIZE:
                                    ok_users += 1
                                else:
                                    problem_users += 1
                                    csv_writer.writerow(
                                        {
                                            'id': user['id_str'],
                                            'problem': 'img_incorrect_size',
                                            'problem_info': str(img_size)
                                        }
                                    )                                
                            except:
                                problem_users += 1
                                csv_writer.writerow(
                                    {
                                        'id': user['id_str'],
                                        'problem': 'img_missing',
                                        'problem_info': ''
                                    }
                                )
                        else:
                            problem_users += 1
                            csv_writer.writerow(
                                {
                                    'id': user['id_str'],
                                    'problem': 'img_missing',
                                    'problem_info': ''
                                }
                            )
                    else:
                        problem_users += 1
                        csv_writer.writerow(
                            {
                                'id': user['id_str'],
                                'problem': 'img_incorrect_path',
                                'problem_info': img_path
                            }
                        )
                pbar.update(1)                
    print('Users with ok images: {0:,} ({1}%)'.\
        format(ok_users, round(ok_users/total_users,0)))
    print('\n\n')
    print('Users with problems: {0:,} ({1}%)'.\
        format(problem_users, round(problem_users/total_users,0)))
    print('\n\n')
    print('Checkout {}'.format(output_file_name))


def update_user_lang(collection, config_fn=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {
        'lang': None
    }
    projection = {
        '_id': 0,
        'id': 1,        
        'lang': 1
    }
    print('Retriving users...')
    users = list(dbm.find_all(query, projection))
    total_users = len(users)
    print('Fetched {0:,} users'.format(total_users))
    max_batch = BATCH_SIZE if total_users > BATCH_SIZE else total_users
    users_to_update = []
    processing_counter = total_segs = 0
    for user in users:
        start_time = time.time()
        processing_counter += 1
        if 'lang_detected' in user and user['lang_detected'] in const.LANGS:
            users_to_update.append(
                {
                    'filter': {'id': int(user['id'])},
                    'new_values': {'lang': user['lang_detected']}
                }            
            )
        else:
            users_to_update.append(
                {
                    'filter': {'id': int(user['id'])},
                    'new_values': {'lang': const.UNKNOWN_LANG}
                }            
            )
        if len(users_to_update) >= max_batch:
            add_fields(dbm, users_to_update)
            users_to_update = []
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_users)
    if len(users_to_update) > 0:
        add_fields(dbm, users_to_update)


def fix_user_lang(collection, config_fn=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {}
    projection = {
        '_id': 0,
        'id': 1,        
        'lang': 1
    }
    print('Retriving users...')
    users = list(dbm.find_all(query, projection))
    total_users = len(users)
    print('Fetched {0:,} users'.format(total_users))
    max_batch = BATCH_SIZE if total_users > BATCH_SIZE else total_users
    users_to_update = []
    processing_counter = total_segs = 0
    for user in users:
        start_time = time.time()
        processing_counter += 1
        if user['lang'] in consts.LANGS:
            continue
        users_to_update.append(
            {
                'filter': {'id': int(user['id'])},
                'new_values': {'lang': consts.UNKNOWN_LANG}
            }            
        )
        if len(users_to_update) >= max_batch:
            add_fields(dbm, users_to_update)
            users_to_update = []
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_users)
    if len(users_to_update) > 0:
        add_fields(dbm, users_to_update)


def update_user_demo_tweets(collection_tweets, collection_users, config_fn=None):
    dbm_tweets = DBManager(collection=collection_tweets, config_fn=config_fn)
    dbm_users = DBManager(collection=collection_users, config_fn=config_fn)
    query = {
        'exists': 1
    }
    projection = {
        '_id': 0,
        'id': 1,
        'prediction': 1,
        'age_range': 1,
        'type': 1,
        'gender': 1
    }
    print('Retriving users...')
    users = list(dbm_users.find_all(query, projection))
    total_users = len(users)
    print('Fetched {0:,} users'.format(total_users))
    processing_counter = total_segs = 0
    for user in users:
        start_time = time.time()
        processing_counter += 1
        if 'prediction' in user and \
            (user['prediction'] == 'succeded' or user['prediction'] == 'success'):
            print('Updating tweets of the user: {}'.format(user['id']))
            dbm_tweets.update_record_many(
                {
                    'user.id': int(user['id'])
                },
                {
                    'user.prediction': user['prediction'],
                    'user.age_range': user['age_range'],                        
                    'user.gender': user['gender'],
                    'user.type': user['type'],
                    'updated_user': 1
                }
            )
        total_segs = calculate_remaining_execution_time(start_time, total_segs,
                                                        processing_counter, 
                                                        total_users)


def remove_tweets_from_text(search_string, collection, del_collection=None, 
                            config_fn=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)    
    query = {
        '$text': {
            '$search': search_string
        }
    }
    if del_collection:
        dbm_del = DBManager(collection=del_collection, config_fn=config_fn)
        print('Fetching tweets...')
        tweets_to_del = list(dbm.find_all(query))
        if len(tweets_to_del) > 0:
            print('Inserting {} tweets in the delete collection...'.format(len(tweets_to_del)))
            dbm_del.insert_many(tweets_to_del, ordered=False)
    print('Deleting tweets, please wait...')
    result = dbm.remove_records(query)
    print('The process has removed {} tweets'.format(result.deleted_count))    


def do_create_field_created_at_date(collection, config_fn=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)    
    query = {
        'created_at_date': { '$exists': 0 }
    }
    projection = {
        '_id': 0,
        'id_str': 1,
        'created_at': 1
    }
    print('Retriving tweets...')
    tweets = list(dbm.find_all(query, projection))
    print(f'Processing {len(tweets)} tweets')
    tweets_to_update = []
    for tweet in tweets:
        print(f'Processing tweet: {tweet["id_str"]}')
        tweet_date = datetime.strptime(tweet['created_at'], '%a %b %d %H:%M:%S %z %Y')
        if int(tweet_date.month) < 10:
            tweet_month = f'0{tweet_date.month}'
        else:
            tweet_month = tweet_date.month
        if int(tweet_date.day) < 10:
            tweet_day = f'0{tweet_date.day}'
        else:
            tweet_day = tweet_date.day
        created_at_date = f'{tweet_date.year}-{tweet_month}-{tweet_day}'         
        tweets_to_update.append(
            {
                'filter': {'id_str': tweet['id_str']},
                'new_values': {'created_at_date': created_at_date}
            }
        )
    add_fields(dbm, tweets_to_update)


def remove_user(user_screen_name, dbm_tweets, dbm_users):
    """
        Remove the user given by the user_screen_name and all
        of his/her tweets
    """
    # Remove tweets from the user
    result = dbm_tweets.remove_records({
        'user.screen_name': user_screen_name
    })
    print(f'In total {result.deleted_count} tweets from {user_screen_name} were removed')
    # Remove user
    dbm_users.remove_record({
        'screen_name': user_screen_name
    })
    print(f'The user was removed')


def remove_users(banned_accounts_fn, tweets_collection, users_collection, 
                 config_fn=None):
    dbm_tweets = DBManager(collection=tweets_collection, config_fn=config_fn)
    dbm_users = DBManager(collection=users_collection, config_fn=config_fn)
    with open(banned_accounts_fn, 'r') as f:
        accounts = f.readlines() 
        for account in accounts:
            account = account.replace('\n', '')
            print(f'Removing the user: {account} and his/her tweets')
            remove_user(account, dbm_tweets, dbm_users) 


def is_the_total_tweets_above_median(collection, str_date, time_window_in_days, config_fn=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {}
    projection = {
        '_id': 0,
        'id': 1,
        'created_at_date': 1
    }
    time_window_in_days = int(time_window_in_days)
    data = dbm.get_tweets_reduced(query, projection)
    tweets_df = pd.DataFrame(data)
    tweets_df['created_at_date'] = pd.to_datetime(tweets_df['created_at_date']).dt.date
    tweets_by_date = tweets_df.groupby('created_at_date', as_index=False)['id'].count()
    ref_date = max(tweets_by_date['created_at_date']) - timedelta(days=time_window_in_days)
    median_last_days = tweets_by_date[tweets_by_date['created_at_date'] > ref_date]['id'].median()
    print(f'Median of tweets of the last {time_window_in_days} days: {median_last_days}')
    datetime_obj = datetime.strptime(str_date, '%Y-%m-%d')
    date_obj = date(datetime_obj.year, datetime_obj.month, datetime_obj.day)
    num_tweets_date = tweets_by_date.loc[tweets_by_date['created_at_date']==date_obj, 'id'].values[0]
    print(f'Number of tweets published in {str_date}: {num_tweets_date}')
    if num_tweets_date > median_last_days:
        return True
    else:
        return False


def add_status_users(dbm_tweets, users, status):
    total_users = len(users)
    processed_users = 0
    for user in users:
        processed_users += 1
        user_screen_name = user['screen_name']
        print(f'[{processed_users}/{total_users}] Updating status of the user {user_screen_name}')
        dbm_tweets.update_record_many({'user.screen_name': user_screen_name}, 
                                      {'user.exists': status})


def add_status_inactive_users_in_tweets(tweets_collection, users_collection, 
                                        config_fn=None):
    dbm_tweets = DBManager(collection=tweets_collection, config_fn=config_fn)
    dbm_users = DBManager(collection=users_collection, config_fn=config_fn)
    query = {'exists': 0}
    projection = {
        '_id': 0,
        'id': 1,
        'screen_name': 1
    }
    print('Getting inactive users...')
    inactive_users = list(dbm_users.find_all(query, projection))
    print(f'Found {len(inactive_users)} inactive users')
    add_status_users(dbm_tweets, inactive_users, 0)
    

def add_status_active_users_in_tweets(tweets_collection, users_collection, 
                                      config_fn=None):
    dbm_tweets = DBManager(collection=tweets_collection, config_fn=config_fn)
    dbm_users = DBManager(collection=users_collection, config_fn=config_fn)
    query = {'exists': 1}
    projection = {
        '_id': 0,
        'id': 1,
        'screen_name': 1
    }
    print('Getting active users...')
    active_users = list(dbm_users.find_all(query, projection))
    print(f'Found {len(active_users)} active users')
    add_status_users(dbm_tweets, active_users, 1)


def process_user_updates(user_ids, dbm_users, twm):
    existing_users, update_queries = [], []
    for user_obj in twm.user_lookup(user_ids):
        update_queries.append(
            {
                'filter': {'id_str': user_obj['id_str']},
                'new_values': {'exists': 1}
            }                        
        )
        existing_users.append(str(user_obj['id']))
    miss_users = set(user_ids) - set(existing_users)
    logging.info('Out of the {} users searched, {} '\
                 'do not exist anymore'.format(len(user_ids),len(miss_users)))
    for miss_user in miss_users:
        update_queries.append(
            {
                'filter': {'id_str': str(miss_user)},
                'new_values': {'exists': 0}
            }                        
        )
    if len(update_queries) > 0:
        add_fields(dbm_users, update_queries)


def update_user_status(users_collection, config_fn):
    twm = get_twm_obj()
    dbm_users = DBManager(collection=users_collection, config_fn=config_fn)
    query = {}
    projection = {
        '_id': 0,
        'id': 1,
        'screen_name': 1
    }
    logging.info('Getting users...')
    users = list(dbm_users.find_all(query, projection))
    total_users = len(users)
    max_batch = BATCH_SIZE if total_users > BATCH_SIZE else total_users
    user_ids = []
    processing_counter = 0
    for user in users:
        processing_counter += 1
        logging.info('[{}/{}] Processing user: {}'.format(processing_counter, \
                     total_users, user['screen_name']))
        user_ids.append(str(user['id']))
        if len(user_ids) == max_batch:
            process_user_updates(user_ids, dbm_users, twm)
            user_ids = []
    if len(user_ids) > 0:
        process_user_updates(user_ids, dbm_users, twm)


def identify_users_from_outside_spain(collection, config_fn=None):
    la_locations = ['México', 'Perú', 'Argentina', 'Buenos Aire', 'Colombia', 
                    'Venezuela', 'San Salvador', 'El Salvador', 'Costa Rica', 
                    'Guanajuato', 'Ecuador', 'Jalisco', 'Guadalajara',
                    'Monterrey', 'Paraguay', 'Chile', 'Uruguay', 'Bolivia',
                    'Brasil', 'Santo Domingo', 'Dominicana', 'Cuba', 'Honduras',
                    'Panamá', 'India', 'Pakistan', 'Nigeria', 'USA', 'Mx',
                    'Brazil', 'Peru', 'Panama', 'Francia', 'Italia', 'France',
                    'Italy', 'Germany', 'Alemania', 'Yucatan', 'Yucatán',
                    'Michoacán', 'Michoacan']
    esp_locations = ['España', 'Madrid', 'Barcelona', 'Sevilla', 'Castilla', 
                     'Spain', 'Murcia', 'Alcala', 'Catalunya', 'Galicia',
                     'Pontevedra', 'Bizkaia', 'Andalucia', 'Ourense', 'Alcalá',
                     'Arousa', 'Granada', 'Valladolid', 'Albacete', 'Aragon',
                     'Aragón', 'Canarias', 'Coruña']
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {}
    projection = {
        '_id': 0,
        'id': 1,
        'screen_name': 1,
        'location': 1,
        'description': 1
    }
    print('Getting users...')
    users = list(dbm.find_all(query, projection))
    total_users = len(users)
    processing_counter = 0
    identified_users = 0
    output_file = os.path.join('..','data', 'outside_users.csv')
    with open(output_file, 'w') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=['screen_name', 'location', 'description'])
        csv_writer.writeheader()
        for user in users:
            processing_counter += 1
            logging.info('[{}/{}] Processing user: {}'.format(processing_counter, \
                        total_users, user['screen_name']))
            if user['location']:
                found_esp_location = False
                for esp_location in esp_locations:
                    if esp_location.lower() in user['location'].lower():
                        found_esp_location = True
                        break
                if not found_esp_location:
                    for location in la_locations:                
                            if location.lower() in user['location'].lower():
                                csv_writer.writerow(
                                    {
                                        'screen_name': user['screen_name'],
                                        'location': user['location'],
                                        'description': user['description']
                                    }
                                )
                                identified_users += 1
                                break
    print(f'In total {identified_users} users were identified as belonging to Latinamerica')
    print(f'Take a look at {output_file} for more detailes')


def add_user_lang_flag(users_collection, tweets_collection, config_fn=None):
    dbm_users = DBManager(collection=users_collection, config_fn=config_fn)
    dbm_tweets = DBManager(collection=tweets_collection, config_fn=config_fn)
    query = {
        'comunidad_autonoma': 'desconocido'
    }
    projection = {
        '_id': 0,
        'id_str': 1,
        'screen_name': 1,
        'lang_description': 1
    }
    logging.info('Getting users...')
    users = list(dbm_users.find_all(query, projection))
    total_users = len(users)
    processing_counter = 0
    update_queries = []
    max_batch = BATCH_SIZE if total_users > BATCH_SIZE else total_users
    for user in users:
        processing_counter += 1
        logging.info('[{}/{}] Processing user: {}'.format(processing_counter, \
                     total_users, user['screen_name']))
        # get tweets of user
        query = {'user.screen_name': user['screen_name']}
        projection = {'_id': 0, 'id_str': 1, 'complete_text': 1}
        tweets = list(dbm_tweets.find_all(query, projection))
        total_tweets = len(tweets)
        logging.info('Found {} tweets of the user {}'.format(total_tweets, user['screen_name']))
        if total_tweets > 0:
            lang_detection = defaultdict(int)
            for tweet in tweets:
                tweet_text = tweet['complete_text']
                lang_detected = detect_language(tweet_text)
                lang_detection[lang_detected['pref_lang']] += 1
            lang_detection = sorted(lang_detection.items(), key=lambda x: x[1], reverse=True)
            main_lang = lang_detection[0][0]
        else:
            main_lang = user['lang_description']
        logging.info('The user {0} speaks primarily {1}'.format(user['screen_name'], main_lang))
        update_queries.append(
            {
                'filter': {'id_str': user['id_str']},
                'new_values': {'lang_detected': main_lang}
            }
        )
        if len(update_queries) >= max_batch:
            add_fields(dbm_users, update_queries)
            update_queries = []
    if len(update_queries) >= max_batch:
        add_fields(dbm_users, update_queries)


def remove_users_without_tweets(users_collection, tweets_collection, 
                                old_tweets_collection, config_fn):
    """
    Remove users who don't have tweets
    """
    dbm_users = DBManager(collection=users_collection, config_fn=config_fn)
    dbm_tweets = DBManager(collection=tweets_collection, config_fn=config_fn)
    dbm_old_tweets = DBManager(collection=old_tweets_collection, config_fn=config_fn)
    query = {}
    projection = {
        '_id': 0,
        'id_str': 1,
        'screen_name': 1,
        'comunidad_autonoma': 1
    }
    logging.info('Getting users...')
    users = list(dbm_users.find_all(query, projection))
    total_users = len(users)
    processing_counter = 0
    removed_users = 0
    for user in users:
        processing_counter += 1
        logging.info('[{}/{}] Processing user: {}'.format(processing_counter, \
                     total_users, user['screen_name']))
        # get tweets of user
        query = {'user.screen_name': user['screen_name']}
        projection = {'_id': 0, 'id': 1}
        tweets = list(dbm_tweets.find_all(query, projection))
        total_tweets = len(tweets)
        if total_tweets == 0:
            logging.info('The user {} does not have tweets in the current db'.\
                         format(user['screen_name']))
            if user['comunidad_autonoma'] != 'desconocido':
                logging.info('However, he/she has assigned an autonomous '\
                            'community, so his/her tweets will be copy from '\
                            'the old database of tweets')                    
                old_tweets = list(dbm_old_tweets.find_all(query, {}))
                result = dbm_tweets.insert_many(old_tweets)
                logging.info('It was inserted {} new tweets'.format(len(result.inserted_ids)))
            else:
                logging.info('The user will be removed')           
                dbm_users.remove_record({
                    'screen_name': user['screen_name']
                })                
                removed_users += 1
    logging.info('In total {} users were removed because they dont have tweets '\
                 'in the database')


def generate_word_embeddings(collection, config_fn=None):
    current_path = pathlib.Path(__file__).parent.resolve()
    corpus_fn = os.path.join(current_path, '..', 'data', 'corpus_tweets.csv')
    corpus = []
    if not os.path.isfile(corpus_fn):
        # If corpus doesn't exist let's build it
        dbm = DBManager(collection=collection, config_fn=config_fn)
        query = {
            'type': {'$ne': 'retweet'}
        }
        projection = {
            '_id': 0,
            'id_str': 1,
            'type': 1,
            'complete_text': 1
        }
        PAGE_SIZE = 70000
        page_num = 0
        # Build corpus of tweets
        logging.info('Building corpus of tweets...')
        try:
            with open(corpus_fn, 'w') as f:
                headers = ['id_str', 'type', 'complete_text']
                csv_writer = csv.DictWriter(f, fieldnames=headers, delimiter='\t')
                csv_writer.writeheader()
                while True:        
                    page_num += 1
                    pagination = {'page_num': page_num, 'page_size': PAGE_SIZE}
                    logging.info('Retrieving tweets...')
                    tweets = list(dbm.find_all(query=query, projection=projection, 
                                            pagination=pagination))
                    total_tweets = len(tweets)
                    logging.info('Found {:,} tweets'.format(total_tweets))
                    if total_tweets == 0:
                        break
                    for tweet in tweets:
                        logging.info('Adding tweets to corpus...')
                        if 'complete_text' in tweet:
                            corpus.append(tweet['complete_text'])
                            csv_writer.writerow(tweet)
                    logging.info('Corpus current size: {:,} tweets'.format(len(corpus)))
        except (AutoReconnect, ExecutionTimeout, NetworkTimeout):
            logging.info('Timeout exception, proceeding with the generation of '\
                        'embeddings with a corpus of {:,} tweets'.format(len(corpus)))
    else:
        # If corpus exists let's read it
        with open(corpus_fn, 'r') as f:
            csv_reader = csv.DictReader(f, delimiter='\t')
            for row in csv_reader:
                corpus.append(row['complete_text'])
    # Generate embeddings
    logging.info('Starting the generation of embeddings...')
    et = EmbeddingsTrainer()
    et.load_corpus(corpus)
    et.train(workers=6)
    logging.info('Embeddings generation finished, saving model...')
    model_fn = os.path.join(current_path, '..', 'data', 'tweets-covid') 
    et.save_model(model_fn)
    logging.info('Model saved in {}'.format(model_fn))


def find_similar_words_to_terms(terms_list):
    current_path = pathlib.Path(__file__).parent.resolve()
    root_path = current_path.parents[0]
    model_fn = os.path.join(root_path, 'models', 'tweets-embeddings-model') 
    et = EmbeddingsTrainer()
    et.load_model(model_fn)
    similar_words = et.find_similar(terms_list, max_similar=50)
    return similar_words

#if __name__ == "__main__":
#remove_users('../data/banned_accounts.txt', 'processed_new', 'users', 
#             'config_mongo_inb.json')
#create_field_created_at_date('rc_all', 'config_mongo_inb.json')
#is_the_total_tweets_above_median('rc_all', '2020-09-29', 15, 'config_mongo_inb.json')
#add_status_active_users_in_tweets('processed_new', 'users', 'config_mongo_inb.json')
#update_user_status('users', 'config_mongo_inb.json')
#identify_users_from_outside_spain('users', 'config_mongo_inb.json')
#infer_location_from_demonyms_in_description('users', 'src/config_mongo_inb.json')
#infer_location_from_description_lang('users', 'config_mongo_inb.json')
#add_user_lang_flag('users', 'processed_new', 'config_mongo_inb.json')
#remove_users_without_tweets('users', 'processed_new', 'processed', 
#                            'config_mongo_inb.json')
#add_esp_location_flags('users', 'config_mongo_inb.json')
#terms = ['vendo', 'oferta', 'vende']
#similar_words = find_similar_words_to_terms(terms)
#print(similar_words)
#current_path = pathlib.Path(__file__).parent.resolve()
#places_esp_fn = os.path.join(current_path, '..', 'data', 'places_spain.json')
# places_esp_csv_fn = os.path.join(current_path, '..', 'data', 'places_spain_new.csv')
#detector = LocationDetector(places_esp_fn)
#detector.from_csv_to_json(places_esp_csv_fn, places_esp_fn)
#location = '🇪🇺🇪🇸🇬🇧'
#description = 'Dona castellonera, mare de dues dragones, metgessa de família. In mens sana, corpore sano'
#ret = detector.identify_place_from_demonyms_in_description(description, location)
#print(ret)
#testset_fn = os.path.join(current_path, '..', 'data', 'location_detector_testset.csv')
#errors_test_fn = os.path.join(current_path, '..', 'data', 'error_evaluation_location_detector.csv')
#detector.evaluate_detector(testset_fn, errors_test_fn)