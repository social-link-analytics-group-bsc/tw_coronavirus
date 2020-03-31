import csv
import logging
import pathlib

from datetime import datetime
from utils.language_detector import detect_language
from utils.db_manager import DBManager


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)


def infer_language(data_folder, input_file_name, sample=False):
    output_file_name = data_folder + '/tweets_languages_' + input_file_name
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
        dt_obj = datetime.strptime(str_tw_dt, '%a %b %d %H:%M:%S %z %Y')        
        tweet['date_time'] = dt_obj.strftime("%m/%d/%Y, %H:%M:%S")
        tweet['date'] = dt_obj.strftime("%m/%d/%Y")
        tweet['time'] = dt_obj.strftime("%H:%M:%S")
        dbm.update_record({'id': tweet_id}, tweet)
