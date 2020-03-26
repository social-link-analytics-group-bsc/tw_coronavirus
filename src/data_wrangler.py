import csv
import logging
import pathlib

from utils.language_detector import detect_language


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)


def infer_language(data_folder, input_file_name, sample=False):
    output_file_name = data_folder + '/tweets_languages_' + input_file_name
    input_file_name = data_folder + '/' + input_file_name
    sample_size = 10

    logging.info('Reading file: {}'.format(input_file_name))
    tweets = []
    with open(input_file_name, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        row_counter = 0
        for row in csv_reader:
            row_counter += 1
            if sample and row_counter > sample_size:
                break
            tweets.append({
                'tweet_id': row['tweet_id'],
                'tweet': row['tweet']
            })

    logging.info('Starting process of infering languages of tweets...')
    total_tweets = len(tweets)
    try:        
        tweet_counter = 0
        tweet_langs = []
        for tweet in tweets:
            tweet_counter += 1
            logging.info('[{0}/{1}] Infering language of tweet: {2}'.\
                         format(tweet_counter, total_tweets, tweet['tweet_id']))
            lang = detect_language(tweet['tweet'])
            tweet_langs.append(
                {'tweet_id': tweet['tweet_id'], 'lang': lang}
            )         
    except Exception as e:
        logging.exception(f'The following error occurs when infering language '\
                           'of tweets')
    finally:
        logging.info('Saving results to file: {}'.format(output_file_name))
        with open(output_file_name, 'w') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=['tweet_id', 'lang'])
            csv_writer.writeheader()
            for tweet_lang in tweet_langs:
                csv_writer.writerow(tweet_lang)