import csv
import json
import logging
import pathlib
import os

from collections import defaultdict
from random import seed, random
from utils.db_manager import DBManager
from utils.sentiment_analyzer import SentimentAnalyzer
from utils.utils import exists_user


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)


def save_tweet_sentiment_scores_to_csv(sentiment_file):
    dbm = DBManager(collection='tweets_esp_hpai')
    sa = SentimentAnalyzer()
    # get tweets from file
    tweets_selected = []
    with open(sentiment_file, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            tweet = dbm.find_record(
                {'sentiment_score': float(row['score']),
                 '$or': [
                     {'text': row['texto']},
                     {'extended_tweet.full_text': row['texto']}
                 ]                                     
                }
            )
            if tweet:
                tweet_text = tweet['extended_tweet']['full_text'] \
                    if 'extended_tweet' in tweet else tweet['text']
                sentiment_score_affin = sa.normalize_score(tweet['sentiment_score_affin']) \
                    if 'sentiment_score_affin' in tweet else ''
                sentiment_score_sentipy = sa.normalize_score(tweet['sentiment_score_sentipy']) \
                    if  'sentiment_score_sentipy' in tweet else ''
                sentiment_polyglot = sa.normalize_score(tweet['sentiment_score_polyglot']) \
                    if 'sentiment_score_polyglot' in tweet and tweet['sentiment_score_polyglot'] else ''
                tweets_selected.append(
                    {
                        'id': tweet['id'],
                        'texto': tweet_text,
                        'score_polyglot': sentiment_polyglot,
                        'score_affin': sentiment_score_affin,
                        'score_sentipy': sentiment_score_sentipy,
                        'final_score': tweet['sentiment_score']
                    }
                )
            else:
                logging.info('Could not find the tweet {}'.format(row['texto']))
    logging.debug('Recovered {} tweets'.format(len(tweets_selected)))
    output_file = '../data/bsc/processing_outputs/sentiment_analysis_sample_scores.csv'
    logging.info('Saving tweets to the CSV {}'.format(output_file))
    with open(output_file, 'w') as csv_file:
        headers = tweets_selected[0].keys()
        csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
        csv_writer.writeheader()
        for tweet_selected in tweets_selected:
            csv_writer.writerow(tweet_selected)


def export_sentiment_sample(sample_size, collection, config_fn=None, 
                            output_filename=None, lang=None):
    current_path = pathlib.Path(__file__).resolve()
    project_dir = current_path.parents[1]
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {
    }
    projection = {
        '_id': 0,
        'id': 1,
        'complete_text': 1,
        'sentiment.score': 1,
        'created_at_date': 1,
        'lang': 1
    }
    seed(1)
    logging.info('Retrieving tweets...')
    tweets = dbm.find_all(query, projection)
    total_tweets = tweets.count()
    logging.info('Found {} tweets'.format(total_tweets))
    if not output_filename:
        output_filename = 'sentiment_analysis_sample.csv'
    output_file = os.path.join(project_dir, 'data', output_filename)
    logging.info('Processing and saving tweets into {}'.format(output_file))
    sample_size = int(sample_size)
    saved_tweets = 0
    tweets_by_date = defaultdict(int)
    MAX_TWEETS_BY_DATE = 6
    with open(output_file, 'w') as csv_file:
        csv_writer = csv.DictWriter(csv_file, 
                                    fieldnames=['id', 'date', 'lang', 'texto', 'score'])
        csv_writer.writeheader()
        for tweet in tweets:
            if lang and tweet['lang'] != lang:
                continue
            if random() > 0.5:
                if tweets_by_date[tweet['created_at_date']] <= MAX_TWEETS_BY_DATE:                                    
                    saved_tweets += 1
                    tweets_by_date[tweet['created_at_date']] += 1
                    csv_writer.writerow(
                        {
                            'id': tweet['id'],
                            'date': tweet['created_at_date'],
                            'lang': tweet['lang'],
                            'texto': tweet['complete_text'],
                            'score': tweet['sentiment']['score']
                        }
                    )
            if saved_tweets == sample_size:
                break


def export_user_sample(sample_size, collection, config_file=None, output_filename=None):
    project_dir = pathlib.Path(__file__).parents[1].resolve()
    if not output_filename:
        output_filename = 'user_sample.jsonl'
    output = os.path.join(project_dir, 'data', output_filename)
    dbm = DBManager(collection=collection, config_fn=config_file)
    query_filter = {
        'lang': 'es'
    }    
    projection = {
        '_id': 0, 
        'user': 1
    }
    logging.info('Getting sample of users, please wait...')
    tweets = dbm.get_sample(int(sample_size), query_filter, projection)
    total_tweets = len(tweets)
    logging.info('Found {} users'.format(total_tweets))
    saved_tweets = 0
    with open(output, 'w') as f:
        for i in range(total_tweets):
            user_obj = tweets[i]['user']
            if exists_user(user_obj):            
                saved_tweets += 1
                logging.info('[{0}] Saving user: {1}'.format(saved_tweets, user_obj['screen_name']))
                f.write("{}\n".format(json.dumps(user_obj)))                


def do_export_users(collection, config_file=None, output_filename=None):
    project_dir = pathlib.Path(__file__).parents[1].resolve()
    if not output_filename:
        output_filename = 'users.jsonl'
    output = os.path.join(project_dir, 'data', output_filename)
    dbm = DBManager(collection=collection, config_fn=config_file)
    query = {
        'exists': 1
    }
    projection = {
        '_id': 0,
        'id': 1,
        'name': 1,
        'screen_name': 1,
        'description': 1,
        'lang': 1,
        'img_path': 1
    }
    logging.info('Retrieving users...')
    users = list(dbm.find_all(query, projection))
    total_users = len(users)
    logging.info('Found {} users'.format(total_users))
    with open(output, 'w') as f:
        for user in users:
            if 'predicted' in user:
                continue
            if 'img_path' not in user:
                continue
            if user['img_path'] == '[no_img]':
                continue
            if 'lang' in user and user['lang'] == None:
                user['lang'] = 'un'
            logging.info('Exporting user: {}'.format(user['screen_name']))
            f.write("{}\n".format(json.dumps(user)))
    logging.info('Process finished, output was saved into {}'.format(output))


if __name__ == "__main__":
    export_sentiment_sample(600, 'processed_new', 'config_mongo_inb.json', 
                            'sentiment_analysis_sample_es.csv', 'es')