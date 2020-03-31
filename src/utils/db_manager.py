from collections import defaultdict
from pymongo import MongoClient
from utils.utils import get_config

import pathlib

class DBManager:
    __collection = ''

    def __init__(self, collection, db_name = ""):
        script_parent_dir = pathlib.Path(__file__).parents[1]
        config_fn = script_parent_dir.joinpath('config.json')
        config = get_config(config_fn)
        connection_dict = {
            'host': config['mongodb']['host'],
            'port': config['mongodb']['port']
        }
        if config['mongodb']['username']:
            connection_dict.update({
                'username': config['mongodb']['username']
            })
        if config['mongodb']['password']:
            connection_dict.update({
                'password': config['mongodb']['password']
            })
        client = MongoClient(**connection_dict)
        if not db_name:
            self.__db = client[config['mongo']['db_name']]
        else:
            self.__db = client[db_name]
        self.__collection = collection

    def num_records_collection(self):
        return self.__db[self.__collection].find({}).count()

    def clear_collection(self):
        self.__db[self.__collection].remove({})

    def save_record(self, record_to_save):
        self.__db[self.__collection].insert(record_to_save)

    def find_record(self, query):
        return self.__db[self.__collection].find_one(query)

    def update_record(self, filter_query, new_values, create_if_doesnt_exist=False):
        return self.__db[self.__collection].update_one(filter_query, {'$set': new_values},
                                                       upsert=create_if_doesnt_exist)

    def update_record_many(self, filter_query, update_query, create_if_doesnt_exist=False):
        return self.__db[self.__collection].update_many(filter_query, update_query,
                                                       upsert=create_if_doesnt_exist)

    def remove_field(self, filter_query, old_values, create_if_doesnt_exist=False):
        return self.__db[self.__collection].update_one(filter_query, {'$unset': old_values},
                                                       upsert=create_if_doesnt_exist)

    def search(self, query):
        return self.__db[self.__collection].find(query, no_cursor_timeout=True)

    def search_one(self, query, i):
        return self.__db[self.__collection].find(query)[i]

    def remove_record(self, query):
        self.__db[self.__collection].delete_one(query)

    def find_tweets_by_author(self, author_screen_name, **kwargs):
        query = {'user.screen_name': author_screen_name}
        if 'limited_to_time_window' in kwargs.keys():
            query.update({'extraction_date': {'$in': kwargs['limited_to_time_window']}})
        return self.search(query)

    def find_all(self, projection=None):
        if projection:
            return self.__db[self.__collection].find({}, projection)
        else:
            return self.__db[self.__collection].find()

    def find_tweets_by_hashtag(self, hashtag, **kwargs):
        pass

    def aggregate(self, pipeline):
        return [doc for doc in self.__db[self.__collection].aggregate(pipeline, allowDiskUse=True)]

    def __add_extra_filters(self, match, **kwargs):
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
        match = {}
        pipeline = [
            {
                '$match': match
            },
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
        match = {
            'relevante': {'$eq': 1}
        }
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
    
    def get_tweet_places(self, location_reference, **kwargs):
        match = {
            'relevante': {'$eq': 1}
        }
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
        match = {
            'relevante': {'$eq': 1}
        }
        group = {
            '_id': '$tweet_py_date',
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
            'relevante': {'$eq': 1},
            'tweet_py_date': {'$eq': interested_date}
        }
        group = {
            '_id': '$tweet_py_hour',
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
            'relevante': {'$eq': 1},
            'user.screen_name': {'$eq': username}
        }
        project = {
            '_id': 0,
            'tweet': '$tweet_obj',
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
                    'dateString': '$tweet_py_date'
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
            '_id': '$tweet_py_date',
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