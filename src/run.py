import click
import logging
import os
import pathlib
import sys

from data_exporter import save_tweet_sentiments_to_csv, \
      save_tweet_sentiment_scores_to_csv
from data_wrangler import infer_language, add_date_time_field_tweet_objs, \
      check_datasets_intersection, check_performance_language_detection, \
      compute_sentiment_analysis_tweets, identify_duplicates, \
      add_covid_keywords_flag, add_lang_flag, add_place_flag, sentiment_evaluation, \
      update_sentiment_score_fields, do_drop_collection, do_add_language_flag, \
      add_esp_location_flags, do_add_query_version_flag
from data_loader import upload_tweet_sentiment, do_collection_merging, \
     do_update_collection
from network_analysis import NetworkAnalyzer

# Add the directory to the sys.path
#sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)


def check_current_directory():
    cd_name = os.path.basename(os.getcwd())
    if cd_name != 'src':
        click.UsageError('Illegal use: This script must run from the src directory')


@click.group()
def run():
    pass


@run.command()
@click.argument('tweets_collection_name') # Name of collections that contain tweets
def create_db_users(tweets_collection_name):
    """
    Create a database of users that published tweets
    """
    check_current_directory()
    print('Process of creating the database of users has started, please check the ' \
          'log for updates...')
    na = NetworkAnalyzer(tweets_collection_name)
    na.create_users_db()
    print('Process has finished, results were stored in the collection users in ' \
          'your database.')


@run.command()
def create_interaction_net():
    """
    Create a network of users interactions
    """
    check_current_directory()
    print('Process of creating the network of interactions has started, please check the ' \
          'log for updates...')
    na = NetworkAnalyzer()
    na.generate_network()
    print('Process has finished, results were stored in the directory sna/gefx.')


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def add_date_fields(collection_name, config_file):
    """
    Add date fields to tweet documents
    """
    check_current_directory()
    add_date_time_field_tweet_objs(collection_name, config_file)


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def sentiment_analysis(collection_name, config_file):
    """
    Compute sentiment analysis of tweets
    """
    check_current_directory()
    print('Process of computing sentiment analysis has started, please ' \
          'check the log for updates...')    
    compute_sentiment_analysis_tweets(collection_name, config_file)


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--flag_covid_keywords', help='Run process that add covid keywords flag', \
              default=False, is_flag=True)
@click.option('--flag_place', help='Run process that add place flag', \
              default=False, is_flag=True)
def add_flags(collection_name, config_file, flag_covid_keywords, flag_place):
    """
    Add flags to tweets
    """
    check_current_directory()
    print('Process of adding flags has started, follow updates on the log...')
    if flag_covid_keywords:
        add_covid_keywords_flag(collection_name, config_file)
    if flag_place:
        add_place_flag(collection_name, config_file)


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File name with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--add_date_fields', help='Indicate whether date fields should be created', \
              default=False, is_flag=True)              
def preprocess(collection_name, config_file, add_date_fields):
    """
    Add flags and run sentiment analysis
    """
    check_current_directory()
    print('Pre-processing process has started, follow updates on the log...')
    if add_date_fields:
        add_date_time_field_tweet_objs(collection_name, config_file)
    compute_sentiment_analysis_tweets(collection_name, config_file)

@run.command()
@click.argument('target_collection') # Name of the target collection
@click.argument('source_collection') # Name of the collection to merge
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def merge_collections(target_collection, source_collection, config_file):
    check_current_directory()
    print('Merging process has started, follow updates on the log...')
    do_collection_merging(target_collection, [source_collection], config_file)


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def drop_collection(collection_name, config_file):
    check_current_directory()
    print('Dropping collection...')
    do_drop_collection(collection_name, config_file)


@run.command()
@click.argument('collection_name') # Name of collections to update
@click.argument('source_collection') # Name of collections from where to extract data
@click.argument('end_date') # Date of the last date to consider in the update
@click.option('--start_date', help='Date of the first date to consider in the update', \
              default=None, is_flag=False)
@click.option('--config_file', help='File name with Mongo configuration', \
              default=None, is_flag=False)
def update_collection(collection_name, source_collection, end_date, start_date, 
                      config_file):
    check_current_directory()
    print('Updating collection...')
    do_update_collection(collection_name, source_collection, end_date, 
                         start_date, config_file)


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def add_language_flag(collection_name, config_file):
    """
    Add language flags to Spanish tweets
    """
    check_current_directory()
    print('Detecting language')
    do_add_language_flag(collection_name, config_file)  


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--unknown_loc', help='Indicate whether flags of unknown locations should be updated', \
              default=False, is_flag=True)
def add_location_flags(collection_name, config_file, unknown_loc):
    """
    Add Spain location flags to tweets
    """
    check_current_directory()
    print('Adding location flags')
    add_esp_location_flags(collection_name, config_file, unknown_loc)


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def add_query_version_flag(collection_name, config_file):
    """
    Add query version flag to tweets
    """
    check_current_directory()
    print('Adding query version flag')
    do_add_query_version_flag(collection_name, config_file)


if __name__ == "__main__":
    run()