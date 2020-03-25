import csv
import logging
import pandas as pd
import pathlib

from utils import detect_language


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)


def infer_language(input_file_name):
    output_file_name = 'data/tweets_languages_'+input_file_name
    input_file_name = 'data/' + input_file_name

    logging.info('Reading file: {}'.format(input_file_name))
    tweets = pd.read_csv(input_file_name, usecols=['tweet_id', 'tweet'])
    total_tweets = tweets.shape[0]

    try:
        logging.info('Starting process of infering languages of tweets...')
        tweet_counter = 0
        tweet_langs = []
        for _, tweet in tweets.iterrows():
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
        pd.DataFrame.from_dict(tweet_langs).to_csv(output_file_name, index=False)        


if __name__ == "__main__":
    infer_language('ours_2019-01-01_to_2020-02-22_coronavirus(es-en)_tweets.csv')