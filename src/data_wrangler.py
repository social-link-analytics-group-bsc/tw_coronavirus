import csv
import demoji
import logging
import pathlib
import os
import pandas as pd
import preprocessor as tw_preprocessor
import re
import time

from datetime import datetime, timedelta
from utils.language_detector import detect_language
from utils.db_manager import DBManager
from utils.utils import get_tweet_datetime, SPAIN_LANGUAGES, \
        get_covid_keywords, get_spain_places_regex, get_spain_places, \
        calculate_remaining_execution_time
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


BATCH_SIZE = 5000


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


def compute_sentiment_analysis_tweets(collection, config_fn=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {}
    query.update(
        {        
            'sentiment': {'$exists': 0}
        }
    )
    projection = {
        '_id': 0,
        'id': 1,
        'retweeted_status': 1,
        'text': 1,
        'lang': 1,
        'extended_tweet': 1
    }
    tweets = dbm.find_all(query, projection)
    sa = SentimentAnalyzer()                       
    total_tweets = tweets.count()
    logging.info('Going to compute the sentiment of {0:,} tweets'.format(total_tweets))
    max_batch = BATCH_SIZE if total_tweets > BATCH_SIZE else total_tweets 
    processing_counter = total_segs = 0
    processed_sentiments = {}
    update_queries = []
    for tweet in tweets:
        tweet_id = tweet['id']
        if tweet_id not in processed_sentiments:
            start_time = time.time()
            processing_counter += 1
            logging.info('[{0}/{1}] Computing sentiment of tweet:\n{2}'.\
                        format(processing_counter, total_tweets, tweet['text']))
            if 'retweeted_status' not in tweet:
                sentiment_analysis_ret = compute_sentiment_analysis_tweet(tweet, sa)
                if sentiment_analysis_ret:
                    sentiment_dict = prepare_sentiment_obj(sentiment_analysis_ret)
                    processed_sentiments[tweet_id] = sentiment_dict
            else:
                logging.info('Found a retweet')
                id_org_tweet = tweet['retweeted_status']['id']                
                if id_org_tweet not in processed_sentiments:   
                    original_tweet = tweet['retweeted_status']
                    sentiment_analysis_ret = compute_sentiment_analysis_tweet(original_tweet, sa)
                    if sentiment_analysis_ret:
                        sentiment_dict = prepare_sentiment_obj(sentiment_analysis_ret)
                        processed_sentiments[id_org_tweet] = sentiment_dict
                else:
                    sentiment_dict = processed_sentiments[id_org_tweet]
        else:
            sentiment_dict = processed_sentiments[tweet_id]
        update_queries.append(
            {
                'filter': {'id': int(tweet_id)},
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


def do_add_language_flag(collection, config_fn=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {
        'lang': 'es',
        'lang_detection': {'$exists': 0}
    }
    projection = {
        '_id': 0,
        'id': 1,
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
        tweet_id = tweet['id']
        start_time = time.time()
        processing_counter += 1
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
                'filter': {'id': int(tweet_id)},
                'new_values': new_values
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


def identify_location(locations, places_esp, cities, provinces, ccaas, 
                      ccaa_province):
    found_place, found_esp = False, False
    for location in locations:
        if not location:
            continue
        location = location.lower().strip().replace('.','').\
                    replace('(','').replace(')','').replace('d’','').\
                    replace('L’','').replace(']','').replace('[','')
        if location in cities:
            place = places_esp[places_esp['ciudad'].str.lower()==location]
            found_place=True
        elif location in provinces:
            place = places_esp[places_esp['provincia'].str.lower()==location]
            found_place=True
        elif location in ccaas:
            place = places_esp[places_esp['comunidad autonoma'].str.lower()==location]
            found_place=True
        if found_place:
            ccaa_province['comunidad_autonoma'] = place.iloc[0]['comunidad autonoma']
            ccaa_province['provincia'] = place.iloc[0]['provincia'] 
            break
        else:
            if not found_esp:
                found_esp = location in ['españa', 'spain', 'espanya']
    if not found_place and found_esp:
        ccaa_province['comunidad_autonoma'] = ccaa_province['provincia'] = 'España'


def identify_unknown_locations(locations, places_esp, cities, provinces, ccaas, 
                               ccaa_province ):
    found_place = False
    for location in locations:
        if not location:
            continue
        location = location.lower().strip().replace('.','').\
                   replace('(','').replace(')','').replace('d’','').\
                   replace('L’','').replace(']','').replace('[','')
        for city in cities:
            for sub_city in city.split():
                if location == sub_city:
                    place = places_esp[places_esp['ciudad'].str.lower()==city]
                    found_place = True
                    break
        if not found_place:
            for province in provinces:
                for sub_province in province.split():
                    if location == sub_province:
                        place = places_esp[places_esp['provincia'].str.lower()==province]
                        found_place = True
                        break
        if not found_place:
            for ccaa in ccaas:
                for sub_ccaa in ccaa.split():
                    if location == sub_ccaa:
                        place = places_esp[places_esp['comunidad autonoma'].str.lower()==ccaa]
                        found_place = True
                        break
        if found_place:
            ccaa_province['comunidad_autonoma'] = place.iloc[0]['comunidad autonoma']
            ccaa_province['provincia'] = place.iloc[0]['provincia']


def add_esp_location_flags(collection, config_fn, unknown=False):
    places_esp = pd.read_csv('../data/places_spain.csv')
    ccaas = set(list(places_esp['comunidad autonoma'].str.lower()))
    provinces = set(list(places_esp[places_esp['provincia']!='']['provincia'].dropna().str.lower()))
    cities = set(list(places_esp[places_esp['ciudad']!='']['ciudad'].dropna().str.lower()))
    dbm = DBManager(collection=collection, config_fn=config_fn)
    if unknown:
        query = {        
            'comunidad_autonoma': 'desconocido'
        }
    else:
        query = {        
            'comunidad_autonoma': {'$exists': 0}
        }
    projection = {
        '_id':0,
        'id':1,
        'user.location':1
    }
    tweets = dbm.find_all(query, projection)
    total_tweets = tweets.count()
    logging.info('Processing locations of {0:,} tweets'.format(total_tweets))
    update_queries = []
    max_batch = BATCH_SIZE if total_tweets > BATCH_SIZE else total_tweets
    processing_counter = total_segs = 0
    for tweet in tweets:
        start_time = time.time()
        tweet_id = tweet['id']
        processing_counter += 1        
        user_location = tweet['user']['location']
        ccaa_province = {
            'comunidad_autonoma':'desconocido', 
            'provincia':'desconocido'
        }
        if user_location: 
            user_location = user_location.replace('/',',')
            user_location = user_location.replace('.',',')
            user_location = user_location.replace(' ',',')
            user_location = user_location.replace('-',',')
            locations = user_location.split(',')
            locations = set(locations)
            if not unknown:           
                identify_location(locations, places_esp, cities, provinces, 
                                  ccaas, ccaa_province)
            else:
                identify_unknown_locations(locations, places_esp, cities, provinces,
                                           ccaas, ccaa_province)
        update_queries.append(
            {
                'filter': {'id': int(tweet_id)},
                'new_values': ccaa_province
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
