import csv
import logging
import pathlib

from random import seed, random
from utils.db_manager import DBManager


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)


def save_tweet_sentiments_to_csv():
    dbm = DBManager(collection='tweets_esp_hpai')
    sentiment_tweets = dbm.search({'sentiment_score': {'$exists': 1}})
    positive_tweets, negative_tweets, neutral_tweets = [], [], []
    thresholds = {'low': -0.05, 'high': 0.05}
    max_tweets_to_export = 200
    seed(1)
    output_file = '../data/bsc/processing_outputs/sentiment_analysis_sample.csv'
    logging.info('Collecting tweets, please wait...')
    for tweet in sentiment_tweets:
        if 'extended_tweet' not in tweet:
            continue
        sentiment_score = tweet['sentiment_score']        
        if sentiment_score < thresholds['low']:
            if random() > 0.5 and len(negative_tweets) < max_tweets_to_export:         
                negative_tweets.append(
                    {
                        'texto': tweet['extended_tweet']['full_text'],
                        'score': tweet['sentiment_score'],
                        'sentimiento': 'negativo'
                    }
                )
        elif sentiment_score > thresholds['high']:
            if random() > 0.5 and len(positive_tweets) < max_tweets_to_export:         
                positive_tweets.append(
                    {
                        'texto': tweet['extended_tweet']['full_text'],
                        'score': tweet['sentiment_score'],
                        'sentimiento': 'positivo'
                    }
                )
        else:
            if random() > 0.5 and len(neutral_tweets) < max_tweets_to_export:
                neutral_tweets.append(
                    {
                        'texto': tweet['extended_tweet']['full_text'],
                        'score': tweet['sentiment_score'],
                        'sentimiento': 'neutral'
                    }
                )
        if len(negative_tweets) >= max_tweets_to_export and \
           len(positive_tweets) >= max_tweets_to_export and \
           len(neutral_tweets) >= max_tweets_to_export:
           break
    logging.info('Saving tweets to the CSV {}'.format(output_file))
    with open(output_file, 'w') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=['texto', 'score', 'sentimiento'])
        for i in range(200):
            csv_writer.writerow(positive_tweets[i])
            csv_writer.writerow(negative_tweets[i])
            csv_writer.writerow(neutral_tweets[i])
    