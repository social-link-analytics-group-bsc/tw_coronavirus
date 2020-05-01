from collections import defaultdict
from datetime import datetime
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
from .utils import get_config, get_tweet_datetime

import logging
import pathlib


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[1].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)


class DBManager:
    __collection = ''

    def __init__(self, collection = None, config_fn = None, db_name = None):                
        if not config_fn:
            script_parent_dir = pathlib.Path(__file__).parents[1]
            config_fn = script_parent_dir.joinpath('config.json')        
        config = get_config(config_fn)
        connection_dict = {
            'host': config['mongodb']['host'],
            'port': int(config['mongodb']['port'])
        }
        if 'username' in config['mongodb'] and \
            config['mongodb']['username'] != '':
            connection_dict.update({
                'username': config['mongodb']['username']
            })
        if 'password' in config['mongodb'] and \
            config['mongodb']['password'] != '':
            connection_dict.update({
                'password': config['mongodb']['password'],
                'authSource': config['mongodb']['db_name'],
                'authMechanism': 'DEFAULT'
            })
        client = MongoClient(**connection_dict)
        if not db_name:
            self.__db = client[config['mongodb']['db_name']]
        else:
            self.__db = client[db_name]
        if collection:
            self.__collection = collection

    def create_view(self, name, source, query):
        self.__db.command(
            {
                "create": name,
                "viewOn": source,
                "pipeline": query
            }            
        )

    def create_collection(self, name):
        existing_collections = self.__db.list_collection_names()
        exists_collection = False
        for existing_collection in existing_collections:
            if existing_collection == name:
                exists_collection = True
                break
        if not exists_collection:
            self.__db.create_collection(name)
            self.__collection = name
            return True
        else:
            return False

    def set_collection(self, name):
        self.__collection = name

    def get_db_collections(self):
        return self.__db.list_collection_names()

    def drop_collection(self, name=None):
        if not name:
            return self.__db[self.__collection].drop()
        else:
            return self.__db[name].drop()

    def clear_collection(self):
        self.__db[self.__collection].remove({})

    def num_records_collection(self):
        return self.__db[self.__collection].find({}).count()

    def create_index(self, name, sorting_type='desc', unique=False):
        if sorting_type == 'desc':
            direction = DESCENDING
        else:
            direction = ASCENDING
        self.__db[self.__collection].create_index([(name, direction)], 
                                                  unique=unique)

    def save_record(self, record_to_save):
        self.__db[self.__collection].insert(record_to_save)

    def find_record(self, query):
        return self.__db[self.__collection].find_one(query)

    def update_record(self, filter_query, new_values, create_if_doesnt_exist=False):
        return self.__db[self.__collection].update_one(filter_query, {'$set': new_values},
                                                       upsert=create_if_doesnt_exist)

    def update_record_many(self, filter_query, new_values, create_if_doesnt_exist=False):
        return self.__db[self.__collection].update_many(filter_query, {'$set': new_values},
                                                       upsert=create_if_doesnt_exist)

    def bulk_update(self, update_queries):
        # create list of objects to update
        update_objs = []
        for update_query in update_queries:
            update_objs.append(
                UpdateOne(
                            update_query['filter'], 
                            {'$set': update_query['new_values']}
                          )
            )            
        return self.__db[self.__collection].bulk_write(update_objs)

    def remove_field(self, filter_query, old_values, apply_to_multiple_records=False):
        return self.__db[self.__collection].update(filter_query, {'$unset': old_values},
                                                   multi=apply_to_multiple_records)

    def search(self, query, no_cursor_timeout=True):
        return self.__db[self.__collection].find(query, no_cursor_timeout=no_cursor_timeout)

    def remove_record(self, query):
        self.__db[self.__collection].delete_one(query)

    def find_tweets_by_author(self, author_screen_name, **kwargs):
        query = {'user.screen_name': author_screen_name}
        return self.search(query)

    def find_all(self, query={}, projection=None):
        if projection:
            return self.__db[self.__collection].find(query, projection)
        else:
            return self.__db[self.__collection].find(query)

    def find_tweets_by_hashtag(self, hashtag, **kwargs):
        pass

    def aggregate(self, pipeline):
        return [doc for doc in self.__db[self.__collection].aggregate(pipeline, allowDiskUse=True)]

    def __add_extra_filters(self, match, **kwargs):
        match.update(kwargs)
        return match

    def get_original_tweets(self, **kwargs):
        match = {
            'retweeted_status': {'$exists': 0},
            'in_reply_to_status_id_str': {'$eq': None},
            'is_quote_status': False
        }
        match = self.__add_extra_filters(match, **kwargs)
        pipeline = [{'$match': match}]
        return self.aggregate(pipeline)

    def get_retweets(self, **kwargs):
        match = {
            'retweeted_status': {'$exists': 1},
            'in_reply_to_status_id_str': {'$eq': None},
            'is_quote_status': False
        }
        match = self.__add_extra_filters(match, **kwargs)
        pipeline = [{'$match': match}]
        return self.aggregate(pipeline)

    def get_replies(self, **kwargs):
        match = {
            'retweeted_status': {'$exists': 0},
            'in_reply_to_status_id_str': {'$ne': None},
            'is_quote_status': False
        }
        match = self.__add_extra_filters(match, **kwargs)
        pipeline = [{'$match': match}]
        return self.aggregate(pipeline)

    def get_quotes(self, **kwargs):
        match = {
            'is_quote_status': True
        }
        match = self.__add_extra_filters(match, **kwargs)
        pipeline = [{'$match': match}]
        return self.aggregate(pipeline)
    
    def get_plain_tweets(self, **kwargs):
        match = {
            'entities.media': {'$exists': 0},  # don't have media
            '$or': [{'entities.urls': {'$size': 0}},  # don't have urls
                    {'truncated': True},  # are truncated tweets
                    # are quoted tweets with only one url, which is the original tweet
                    {'$and': [{'is_quote_status': True}, {'entities.urls': {'$size': 1}}]},
                    {'$and': [{'is_quote_status': True}, {'entities.urls': {'$exists': 0}}]}
                    ]
        }
        match = self.__add_extra_filters(match, **kwargs)
        filter_rts = {'$or': [{'retweeted_status': {'$exists': 0}},
                              {'$and': [{'retweeted_status': {'$exists': 1}},
                                        {'is_quote_status': True}]}]}
        filter_videos = {'$or': [{'is_video': {'$exists': 0}}, {'is_video': 0}]}
        pipeline = [{'$match': match}, {'$match': filter_rts}, {'$match': filter_videos}]
        return self.aggregate(pipeline)

    def get_tweets_with_links(self, **kwargs):
        match = {
            'entities.media': {'$exists': 0},  # don't have media
            '$and': [
                {'entities.urls': {'$ne': []}},  # have urls
                {'truncated': False},  # are not truncated tweets
                {'$or': [
                    {'is_quote_status': False},
                    {'$and': [{'is_quote_status': True}, {'entities.urls': {'$size': 2}}]}
                ]}
                ]
        }
        match = self.__add_extra_filters(match, **kwargs)
        filter_rts = {'$or': [{'retweeted_status': {'$exists': 0}},
                              {'$and': [{'retweeted_status': {'$exists': 1}},
                                        {'is_quote_status': True}]}]}
        pipeline = [{'$match': match}, {'$match': filter_rts}]
        return self.aggregate(pipeline)
    
    def get_unique_users(self, **kwargs):
        pipeline = [
            {
                '$group': {
                    '_id': '$user.id_str',
                    'screen_name': {'$first': '$user.screen_name'},
                    'verified': {'$first': '$user.verified'},
                    'location': {'$first': '$user.location'},
                    'url': {'$first': '$user.url'},
                    'name': {'$first': '$user.name'},
                    'description': {'$first': '$user.description'},
                    'followers': {'$first': '$user.followers_count'},
                    'friends': {'$first': '$user.friends_count'},
                    'created_at': {'$first': '$user.created_at'},
                    'time_zone': {'$first': '$user.time_zone'},
                    'geo_enabled': {'$first': '$user.geo_enabled'},
                    'language': {'$first': '$user.lang'},
                    'default_theme_background': {'$last': '$user.default_profile'},
                    'default_profile_image': {'$last': '$user.default_profile_image'},
                    'favourites_count': {'$last': '$user.favourites_count'},
                    'listed_count': {'$last': '$user.listed_count'},
                    'tweets_count': {'$sum': 1},
                    'tweets': {'$push': {'text': '$text',
                                         'mentions': '$entities.user_mentions',
                                         'quote': '$quoted_status_id',
                                         'quoted_user_id': '$quoted_status.user.screen_name',
                                         'reply': '$in_reply_to_status_id_str',
                                         'replied_user_id': '$in_reply_to_screen_name',
                                         'retweet': '$retweeted_status.id_str',
                                         'retweeted_user_id': '$retweeted_status.user.screen_name'
                                         }
                                }
                }
            },
            {
                '$sort': {'tweets_count': -1}
            }
        ]
        results = self.aggregate(pipeline)
        # calculate the number of rts, rps, and qts
        # compute the users' interactions
        for result in results:
            ret_tweets = result['tweets']
            rt_count = 0
            qt_count = 0
            rp_count = 0
            interactions = defaultdict(dict)
            for tweet in ret_tweets:
                if 'retweet' in tweet.keys():
                    rt_count += 1
                    user_id = tweet['retweeted_user_id']
                    if user_id in interactions.keys():
                        interactions[user_id]['total'] += 1
                        if 'retweets' in interactions[user_id].keys():
                            interactions[user_id]['retweets'] += 1
                        else:
                            interactions[user_id].update({'retweets': 1})
                    else:
                        interactions[user_id] = {'retweets': 1, 'total': 1}
                elif 'quote' in tweet.keys():
                    qt_count += 1
                    user_id = tweet['quoted_user_id']
                    if user_id in interactions.keys():
                        interactions[user_id]['total'] += 1
                        if 'quotes' in interactions[user_id].keys():
                            interactions[user_id]['quotes'] += 1
                        else:
                            interactions[user_id].update({'quotes': 1})
                    else:
                        interactions[user_id] = {'quotes': 1, 'total': 1}
                elif tweet['reply'] or tweet['replied_user_id']:
                    rp_count += 1
                    user_id = tweet['replied_user_id']
                    if user_id in interactions.keys():
                        interactions[user_id]['total'] += 1
                        if 'replies' in interactions[user_id].keys():
                            interactions[user_id]['replies'] += 1
                        else:
                            interactions[user_id].update({'replies': 1})
                    else:
                        interactions[user_id] = {'replies': 1, 'total': 1}
                else:
                    if 'mentions' in tweet.keys():
                        mentions = tweet['mentions']
                        for mention in mentions:
                            user_id = mention['screen_name']
                            if user_id in interactions.keys():
                                interactions[user_id]['total'] += 1
                                if 'mentions' in interactions[user_id].keys():
                                    interactions[user_id]['mentions'] += 1
                                else:
                                    interactions[user_id].update({'mentions': 1})
                            else:
                                interactions[user_id] = {'mentions': 1, 'total': 1}
            result['retweets_count'] = rt_count
            result['quotes_count'] = qt_count
            result['replies_count'] = rp_count
            result['original_count'] = result['tweets_count'] - (rt_count+qt_count+rp_count)
            result['interactions'] = interactions
        return results
    
    def get_id_duplicated_tweets(self):
        pipeline = [
            {
                '$group': {
                    '_id': '$id_str',
                    'num_tweets': {'$sum': 1}
                }
            },
            {
                '$match': {
                    'num_tweets': {'$gt': 1}
                }
            },
            {
                '$sort': {'num_tweets': -1}
            }
        ]
        return self.aggregate(pipeline)

    def get_user_and_location(self, **kwargs):
        match = {}
        group = {
            '_id': '$user.id_str',
            'location': {'$first': '$user.location'},
            'description': {'$first': '$user.description'},
            'time_zone': {'$first': '$user.time_zone'},
            'count': {'$sum': 1}
        }       
        pipeline = [
            {'$match': match},
            {'$group': group},
            {'$sort': {'count': -1}}
        ]
        result_docs = self.aggregate(pipeline)
        return result_docs
    
    def get_tweet_places(self, location_reference='place', **kwargs):
        match = {}
        group = {
            'count': {'$sum': 1}
        }
        if location_reference == 'place':
            group.update({'_id': '$place.country'})
        else:
            group.update({'_id': '$user.time_zone'})        
        pipeline = [
            {'$match': match},
            {'$group': group},
            {'$sort': {'count': -1}}
        ]
        result_docs = self.aggregate(pipeline)        
        return result_docs
    
    def get_tweets_by_date(self, **kwargs):
        match = {}
        group = {
            '_id': '$date',
            'num_tweets': {'$sum': 1}
        }
        project = {
            'date': {
                '$dateFromString': {
                    'dateString': '$_id'
                }
            },
            'count': '$num_tweets'
        }
        pipeline = [{'$match': match},
                    {'$group': group},
                    {'$project': project},
                    {'$sort': {'date': 1}}
                    ]
        result_docs = self.aggregate(pipeline)
        return result_docs
    
    def get_tweets_by_hour(self, interested_date, **kwargs):
        match = {
            'date': {'$eq': interested_date}
        }
        group = {
            '_id': '$time',
            'num_tweets': {'$sum': 1}
        }
        project = {
            'hour': '$_id',
            'count': '$num_tweets'
        }
        pipeline = [{'$match': match},
                    {'$group': group},
                    {'$project': project},
                    {'$sort': {'hour': 1}}
                    ]
        result_docs = self.aggregate(pipeline)        
        return result_docs

    def get_tweets_user(self, username):
        match = {
            'user.screen_name': {'$eq': username}
        }
        project = {
            '_id': 0,
            'tweet': '$$ROOT',
            'screen_name': '$user.screen_name'
        }
        pipeline = [
            {'$match': match},
            {'$project': project}
        ]
        search_results = self.aggregate(pipeline)
        results = {'rts': [], 'qts': [], 'rps': [], 'ori': []}
        for result in search_results:
            tweet = result['tweet']
            if 'full_text' in tweet.keys():
                text_tweet = tweet['full_text']
            else:
                text_tweet = tweet['text']
            if 'retweeted_status' in tweet.keys():
                if 'full_text' in tweet['retweeted_status'].keys():
                    text_tweet = tweet['retweeted_status']['full_text']
                else:
                    text_tweet = tweet['retweeted_status']['text']
                results['rts'].append({
                    'author': tweet['retweeted_status']['user']['screen_name'],
                    'original_text': text_tweet,
                    'id_original_text': tweet['retweeted_status']['id_str']
                })
            elif 'quoted_status' in tweet.keys():
                if 'full_text' in tweet['quoted_status'].keys():
                    text_tweet = tweet['quoted_status']['full_text']
                else:
                    text_tweet = tweet['quoted_status']['text']
                results['rts'].append({
                    'author': tweet['quoted_status']['user']['screen_name'],
                    'original_text': text_tweet,
                    'id_original_text': tweet['quoted_status']['id_str']
                })
            elif tweet['in_reply_to_status_id_str']:
                results['rps'].append({
                    'replied_user': tweet['in_reply_to_user_id_str'],
                    'reply': text_tweet,
                    'id_tweet': tweet['id_str']
                })
            else:
                results['ori'].append({'text': text_tweet, 'id_tweet': tweet['id_str']})
        return results
    
    def get_users_and_activity(self, **kwargs):
        match = {}
        pipeline = [
            {'$match': match},
            {'$project': {
                'screen_name': '$screen_name',
                'tweets': '$tweets',
                'original_tweets': '$original_tweets',
                'rts': '$rts',
                'qts': '$qts',
                'rps': '$rps',
                'followers': '$followers',
                'friends': '$friends'
            }},
            {'$sort': {'tweets': -1}}
        ]
        return self.aggregate(pipeline)
    
    def get_posting_frequency_in_seconds(self, **kwargs):
        match = {}
        pipeline = [
            {'$match': match},
            {'$project': {
               'id_str': '$id_str',
               'datetime': {'$dateFromString': {
                    'dateString': '$date_time'
                }},
               '_id': 0,
            }},
            {'$sort': {'datetime': 1}}
        ]
        ret_agg = self.aggregate(pipeline)
        previous_dt = None
        for tweet in ret_agg:
            if not previous_dt:
                previous_dt = tweet['datetime']
                tweet['diff_with_previous'] = 0
                continue
            current_dt = tweet['datetime']
            tweet['diff_with_previous'] = (current_dt - previous_dt).total_seconds()
            previous_dt = current_dt
        return ret_agg
    
    def interactions_user_over_time(self, user_screen_name, **kwargs):
        match = {
            'user.screen_name': {'$ne': user_screen_name}
        }
        group = {
            '_id': '$date',
            'num_tweets': {'$sum': 1}
        }
        project = {
            'date': {
                '$dateFromString': {
                    'dateString': '$_id'
                }
            },
            'count': '$num_tweets'
        }
        # replies
        rp_match = {'in_reply_to_screen_name': {'$eq': user_screen_name}}
        rp_match.update(match)
        rp_project = {'type': 'reply'}
        rp_project.update(project)
        pipeline = [{'$match': rp_match},
                    {'$group': group},
                    {'$project': rp_project},
                    {'$sort': {'date': 1}}]
        results = self.aggregate(pipeline)
        # quotes
        qt_match = {'quoted_status.user.screen_name': {'$eq': user_screen_name}}
        qt_match.update(match)
        qt_project = {'type': 'quote'}
        qt_project.update(project)
        pipeline = [{'$match': qt_match},
                    {'$group': group},
                    {'$project': qt_project},
                    {'$sort': {'date': 1}}]
        results.extend(self.aggregate(pipeline))
        # retweets
        rt_match = {'retweeted_status.user.screen_name': {'$eq': user_screen_name}}
        rt_match.update(match)
        rt_project = {'type': 'retweet'}
        rt_project.update(project)
        pipeline = [{'$match': rt_match},
                    {'$group': group},
                    {'$project': rt_project},
                    {'$sort': {'date': 1}}]
        results.extend(self.aggregate(pipeline))
        return results
    
    def insert_tweet(self, tweet):
        """
        Save a tweet in the database
        :param tweet: dictionary in json format of the tweet
        :return:
        """
        if 'id_str' in tweet:
            id_tweet = tweet['id_str']
        elif 'id' in tweet:
            id_tweet = tweet['id']
        elif 'tweet_id' in tweet:
            id_tweet = tweet['tweet_id']
        else:
            raise Exception('Cannot find id of tweet {}'.format(tweet))
        num_results = self.search({'id': int(id_tweet)}).count()
        if num_results == 0:
            # Add additional date fields
            tw_ct = None
            if 'created_at' in tweet:
                tw_ct = tweet['created_at']
            elif 'formatted_date' in tweet:
                tw_ct = tweet['formatted_date']
            if tw_ct:
                tw_dt, tw_d, tw_t = get_tweet_datetime(tw_ct)
                tweet.update({'date_time': tw_dt, 'date': tw_d, 'time': tw_t})
            self.save_record(tweet)
            logging.info('Inserted tweet: {0}'.format(id_tweet))
            return True
        else:
            logging.info('Tweet duplicated, not inserted')
            return False


    def insert_many_tweets(self, tweets, ordered=True):
        return self.__db[self.__collection].insert_many(tweets, 
                                                        ordered=ordered)
        

    def get_tweets_reduced(self, filters={}, projection={}):        
        results = self.find_all(filters, projection)
        reduced_tweets = []
        special_keys = ['date','date_time','place','sentiment',
                        'retweeted_status', 'is_quote_status',
                        'in_reply_to_status_id_str']
        for tweet in results:
            reduced_tweet = {}
            if 'date' in tweet:
                reduced_tweet['date'] = datetime.strptime(tweet['date'], "%d/%m/%Y")
            if 'date_time' in tweet:
                reduced_tweet['date_time'] = datetime.strptime(tweet['date_time'].replace(',',''), "%d/%m/%Y %H:%M:%S")
            if 'sentiment' in tweet:
                reduced_tweet['sentiment'] = tweet['sentiment']['score']
            if 'place' in tweet and tweet['place']:
                reduced_tweet['place_country'] = tweet['place']['country']
            if 'retweeted_status' in tweet:
                reduced_tweet['type'] = 'rt'
            elif tweet['is_quote_status']:
                reduced_tweet['type'] = 'qt'
            elif tweet['in_reply_to_status_id_str']:
                reduced_tweet['type'] = 'rp'
            else:
                reduced_tweet['type'] = 'og'
            for key, value in tweet.items():
                if key not in special_keys:
                    if isinstance(value,dict):
                        for k, v in value.items():
                            combined_key = key + '_' + k
                            reduced_tweet[combined_key] = tweet[key][k]
                    else:
                        reduced_tweet[key] = tweet[key]
            reduced_tweets.append(reduced_tweet)
        
        return reduced_tweets