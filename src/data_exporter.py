import csv
import demoji
import emoji
import json
import logging
import nltk
import pathlib
import os

from collections import defaultdict
from datetime import datetime
from nltk import wordpunct_tokenize
from nltk.corpus import stopwords
from nltk.stem import SnowballStemmer
from random import seed, random
import preprocessor as tw_preprocessor
from utils.db_manager import DBManager
from utils.sentiment_analyzer import SentimentAnalyzer
from utils.utils import exists_user, check_user_profile_image, week_of_month


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)


# set option of preprocessor
tw_preprocessor.set_options(tw_preprocessor.OPT.URL, 
                            tw_preprocessor.OPT.MENTION, 
                            tw_preprocessor.OPT.HASHTAG,
                            tw_preprocessor.OPT.RESERVED,
                            tw_preprocessor.OPT.NUMBER,
                            tw_preprocessor.OPT.EMOJI)


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


def export_sentiment_scores_from_ids(file_tweet_ids, collection, config_fn):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    tweets_to_export = []
    with open(file_tweet_ids, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            tweet_id = row['id']
            print('Processing tweet: {}'.format(tweet_id))
            tweet_obj = dbm.find_record({'id_str': str(tweet_id)})
            if tweet_obj is None:
                print('Missing tweet...')
                continue
            tweet_to_export = {'id': tweet_id, 'text': tweet_obj['complete_text']}
            sentiment_obj = tweet_obj['sentiment']
            if 'sentiment_score_polyglot' in sentiment_obj:
                tweet_to_export['score_polyglot'] = \
                    sentiment_obj['sentiment_score_polyglot']
            if 'sentiment_score_sentipy' in sentiment_obj:
                tweet_to_export['score_sentipy'] = \
                    sentiment_obj['sentiment_score_sentipy']
            if 'sentiment_score_affin' in sentiment_obj:
                tweet_to_export['score_affin'] = \
                    sentiment_obj['sentiment_score_affin']
            if 'sentiment_score_vader' in sentiment_obj:
                tweet_to_export['score_vader'] = \
                    sentiment_obj['sentiment_score_vader']            
            tweet_to_export['sentiment_score'] = sentiment_obj['score']
            tweet_to_export['human_label'] = row['label']
            tweets_to_export.append(tweet_to_export)
    output_file = '../data/bsc/processing_outputs/sentiment_scores_from_ids.csv'
    print('Saving tweets to the CSV {}'.format(output_file))
    with open(output_file, 'w') as csv_file:
        headers = tweets_to_export[0].keys()
        csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
        csv_writer.writeheader()
        for tweet_to_export in tweets_to_export:
            csv_writer.writerow(tweet_to_export)


def save_tweets_in_csv_file(tweets, output_fn, headers):
    logging.info(f'Saving tweets in the CSV file {output_fn}')
    with open(output_fn, 'w') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=headers)
        csv_writer.writeheader()
        for tweet in tweets:
            dict_row = {
                'id': tweet['id'],
                'date': tweet['created_at_date'],
                'user': tweet['user']['screen_name'],
                'text': tweet['complete_text']
            }
            if 'lang' in headers and 'lang' in tweet:
                dict_row.update({'lang': tweet['lang']})
            if 'sentiment' in headers and 'sentiment' in tweet:
                dict_row.update({'sentiment': tweet['sentiment']['score']})
            if 'retweets' in headers and 'retweet_count' in tweet:
                dict_row.update({'retweets': tweet['retweet_count']})
            if 'favorites' in headers and 'favorite_count' in tweet:
                dict_row.update({'favorites': tweet['favorite_count']})
            if 'replies' in headers and 'reply_count' in tweet:
                dict_row.update({'replies': tweet['reply_count']})
            if 'type' in tweet:
                if 'type' in headers:
                    dict_row.update({'type': tweet['type']})
                if 'original_tweet' in headers:                    
                    if tweet['type'] == 'quote' and 'quoted_status' in tweet:
                        qt_status = tweet['quoted_status']
                        if 'extended_tweet' in qt_status:
                            txt = qt_status['extended_tweet']['full_text']
                        else:
                            txt = qt_status['text']
                        dict_row.update({'original_tweet': txt})
                    else:
                        dict_row.update({'original_tweet': ''})

            csv_writer.writerow(dict_row)            


def export_tweets(collection, output_path, config_fn=None, date=None):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    query = {
    }
    if date:
        query.update({'created_at_date': date})
    projection = {
        '_id': 0,
        'id': 1,
        'user.screen_name': 1,
        'complete_text': 1,
        'sentiment.score': 1,
        'created_at_date': 1,
        'lang': 1,
        'retweet_count': 1,
        'favorite_count': 1,
        'reply_count': 1,
        'type': 1,
        'quoted_status': 1
    }   
    logging.info('Retrieving tweets...')
    tweets = dbm.find_all(query, projection)
    total_tweets = tweets.count()
    logging.info('Found {} tweets'.format(total_tweets))
    if date:
        output_fn = f'tweets_{date}.csv'
    else:
        output_fn = 'tweets.csv'
    output_fn = output_path + output_fn
    output_header = ['id', 'type', 'date', 'user', 'text', 'retweets', \
                     'favorites', 'replies', 'original_tweet']
    save_tweets_in_csv_file(tweets, output_fn, output_header)


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
        'user.screen_name': 1,
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
                                    fieldnames=['id', 'date', 'user', 'text', 'score'])
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
                            'user': tweet['user']['screen_name'],
                            #'lang': tweet['lang'],
                            'text': tweet['complete_text'],
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
        '$and': [ 
            {'prediction': {'$eq': None}}, 
            {'exists': 1}
        ]        
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
    accepted_extensions = ('.png', '.jpg', '.jpeg', '.bmp', '.JPG', '.JPEG', '.PNG', '.BMP')
    with open(output, 'w') as f:
        for user in users:
            if 'prediction' in user:
                logging.info('Found field prediction, ignoring user {}'.format(user['screen_name']))
                continue
            if 'img_path' not in user:
                logging.info('User {} does not have img_path field'.format(user['screen_name']))
                continue
            if user['img_path'] == '[no_img]':
                logging.info('User {} has img_path=[no_img]'.format(user['screen_name']))
                continue
            if not user['img_path'].endswith(accepted_extensions):
                logging.info('User {} has image with extension {}'.format(user['screen_name'], user['img_path']))
                continue
            try:
                img_path = os.path.join(project_dir, user['img_path'])
                check_user_profile_image(img_path)
                logging.info('Exporting user: {}'.format(user['screen_name']))
                f.write("{}\n".format(json.dumps(user)))                
            except Exception as e:
                logging.warning('Error when resizing {0}\nThe error message is: {1}\n'.format(img_path, e))
    logging.info('Process finished, output was saved into {}'.format(output))


def export_tweets_to_json(collection, output_fn, config_fn=None, stemming=False, 
                          lang=None, banned_accounts=[]):    
    query = {
        'type': {'$ne': 'retweet'}
    }
    if lang:
        query.update(
            {
                'lang': {'$eq': lang}
            }   
        )     
    projection = {
        '_id': 0,
        'id': 1, 
        'complete_text': 1,
        'created_at_date': 1,
        'quote_count': 1,
        'reply_count': 1,
        'retweet_count': 1,
        'favorite_count': 1,
        'entities.hashtags': 1,
        'entities.user_mentions': 1,
        'sentiment.score': 1,
        'lang': 1,
        'user.screen_name': 1,
        'comunidad_autonoma': 1,
        'provincia': 1
    }
    if stemming:
        stemmer = SnowballStemmer('spanish')
    PAGE_SIZE = 80000
    page_num = 0
    records_to_read = True
    #processing_counter = total_segs = 0
    while records_to_read:
        page_num += 1
        pagination = {'page_num': page_num, 'page_size': PAGE_SIZE}
        logging.info('Retrieving tweets...')
        dbm = DBManager(collection=collection, config_fn=config_fn)
        tweets = list(dbm.find_all(query, projection, pagination=pagination))
        total_tweets = len(tweets)
        logging.info('Found {:,} tweets'.format(total_tweets))
        if total_tweets == 0:
            break
        with open(output_fn, 'a', encoding='utf-8') as f:
            f.write('[')
            for idx, tweet in enumerate(tweets):
                logging.info('Processing tweet: {}'.format(tweet['id']))
                tweet_txt = tweet['complete_text']
                del tweet['complete_text']
                if tweet['user']['screen_name'] in banned_accounts:
                    continue
                # remove emojis, urls, mentions
                processed_txt = tw_preprocessor.clean(tweet_txt)
                processed_txt = demoji.replace(processed_txt).replace('\u200d️','').strip()
                processed_txt = emoji.get_emoji_regexp().sub(u'', processed_txt)
                tokens = [token.lower() for token in wordpunct_tokenize(processed_txt)]        
                if tweet['lang'] == 'es':
                    stop_words = stopwords.words('spanish')
                    punct_signs = ['.', '[', ']', ',', ';', ')', '),', '(']
                    stop_words.extend(punct_signs)
                    words = [token for token in tokens if token not in stop_words]
                    if stemming:                    
                        stemmers = [stemmer.stem(word) for word in words]
                        processed_txt = ' '.join([stem for stem in stemmers if stem.isalpha() and len(stem) > 1])
                    else:
                        processed_txt = ' '.join(word for word in words)
                else:
                    processed_txt = ' '.join([token for token in tokens])
                tweet['text'] = processed_txt
                if 'sentiment' in tweet:
                    tweet['sentiment_polarity'] = tweet['sentiment']['score']
                    del tweet['sentiment']
                tweet['hashtags'] = []
                for hashtag in tweet['entities']['hashtags']:
                    tweet['hashtags'].append(hashtag['text'])
                #tweet['urls'] = []
                #for url in tweet['entities']['urls']:
                #    tweet['urls'].append(url['expanded_url'])
                tweet['mentions'] = []
                for mention in tweet['entities']['user_mentions']:    
                    tweet['mentions'].append(mention['screen_name'])
                del tweet['entities']
                tweet['url'] = f"http://www.twitter.com/{tweet['user']['screen_name']}/status/{tweet['id']}"
                del tweet['user']
                dt = datetime.strptime(tweet['created_at_date'], '%Y-%m-%d')
                tweet['month'] = dt.month
                tweet['year'] = dt.year
                tweet['week_month'] = f'{week_of_month(dt)}-{dt.month}'
                if idx < (total_tweets-1):
                    f.write('{},\n'.format(json.dumps(tweet, ensure_ascii=False)))
                else:
                    f.write('{}\n'.format(json.dumps(tweet, ensure_ascii=False)))
            f.write(']')    
    

if __name__ == "__main__":
    #export_sentiment_sample(1519, 'rc_all', 'config_mongo_inb.json', lang='es')
    export_tweets_to_json('processed_new', output_fn='../data/all_tweets.json', 
                           config_fn='config_mongo_inb.json')
    #export_tweets('rc_all', '../data/bsc/processing_outputs/', \
    #              'config_mongo_inb.json', '2020-09-12')