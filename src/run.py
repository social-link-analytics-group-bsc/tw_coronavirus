import click
import logging
import os
import pathlib
import sys

from data_exporter import save_tweet_sentiments_to_csv
from data_wrangler import infer_language, add_date_time_field_tweet_objs, \
    check_datasets_intersection, check_performance_language_detection, \
    compute_sentiment_analysis_tweets, assign_sentiments_to_rts, \
    identify_duplicates, add_covid_keywords_flag, add_lang_flag, add_place_flag
from data_loader import upload_tweet_sentiment
from network_analysis import NetworkAnalyzer

# Add the directory to the sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


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
@click.argument('data_dir', type=click.Path(exists=True)) #Path to data directory
@click.argument('tweets_file') #Name of file of the tweets datset
@click.option('--sample', help='Run task on a sample', default=False, is_flag=True)
def detect_language(data_dir, tweets_file, sample):
    """
    Infer language of tweets
    """
    check_current_directory()
    infer_language(data_dir, tweets_file, sample)        


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
def add_date_fields():
    """
    Add date fields to tweet documents
    """
    check_current_directory()
    add_date_time_field_tweet_objs()


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def sentiment_analysis(collection_name, config_file):
    """
    Compute sentiment analysis of tweets
    """
    print('Process of computing sentiment analysis has started, please ' \
          'check the log for updates...')
    compute_sentiment_analysis_tweets(collection_name, config_file)


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--flag_covid_keywords', help='Run process that add covid keywords flag', \
              default=False, is_flag=True)
@click.option('--flag_lang', help='Run process that add lang flag', \
              default=False, is_flag=True)
@click.option('--flag_place', help='Run process that add place flag', \
              default=False, is_flag=True)
def add_flags(collection_name, config_file, flag_covid_keywords, \
              flag_lang, flag_place):
    """
    Add flags to tweets
    """
    print()
    if flag_covid_keywords:
        add_covid_keywords_flag(collection_name, config_file)
    if flag_lang:
        add_lang_flag(collection_name, config_file)
    if flag_place:
        add_place_flag(collection_name, config_file)


if __name__ == "__main__":
    run()