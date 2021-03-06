import click
import logging
import os
import pathlib
import sys

from data_exporter import export_sentiment_sample, \
      save_tweet_sentiment_scores_to_csv, export_user_sample, do_export_users, \
      export_tweets_to_json
from data_wrangler import infer_language, add_date_time_field_tweet_objs, \
      check_datasets_intersection, check_performance_language_detection, \
      compute_sentiment_analysis_tweets, identify_duplicates, \
      add_covid_keywords_flag, add_lang_flag, add_place_flag, sentiment_evaluation, \
      update_sentiment_score_fields, do_drop_collection, do_add_language_flag, \
      add_esp_location_flags, do_add_query_version_flag, update_metric_tweets, \
      do_add_complete_text_flag, do_add_tweet_type_flag, do_update_users_collection, \
      do_update_user_status, do_augment_user_data, compute_user_demographics, \
      compute_user_demographics_from_file, do_create_field_created_at_date, \
      is_the_total_tweets_above_median
from data_loader import upload_tweet_sentiment, do_collection_merging, \
      do_update_collection, do_tweets_replication, load_user_demographics
from network_analysis import NetworkAnalyzer
from pymongo.errors import AutoReconnect, ExecutionTimeout, NetworkTimeout


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
@click.option('--date', help='Date for which the analysis should be run', \
              default=None, is_flag=False)                                          
def sentiment_analysis(collection_name, config_file, date):
    """
    Compute sentiment analysis of tweets
    """
    check_current_directory()
    print('Process of computing sentiment analysis has started, please ' \
          'check the log for updates...')    
    compute_sentiment_analysis_tweets(collection_name, config_file, date=date)


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
@click.option('--source_collection', help='Name of source collection', \
              default=None, is_flag=False)
@click.option('--config_file', help='File name with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--add_date_fields', help='Indicate whether date fields should be created', \
              default=False, is_flag=True)              
def preprocess(collection_name, source_collection, config_file, add_date_fields):
    """
    Add flags and run sentiment analysis
    """
    check_current_directory()
    print('Pre-processing process has started, follow updates on the log...')
    if add_date_fields:
        add_date_time_field_tweet_objs(collection_name, config_file)
    compute_sentiment_analysis_tweets(collection_name, config_file, 
                                      source_collection)

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
@click.option('--source_collection', help='Name of source collection', \
              default=None, is_flag=False)
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--tweets_date', help='Date of tweets that should be updated', \
              default=None, is_flag=False)              
def add_language_flag(collection_name, source_collection, config_file, tweets_date):
    """
    Add language flags to Spanish tweets
    """
    check_current_directory()
    print('Detecting language')
    do_add_language_flag(collection_name, config_file, tweets_date, source_collection)  


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def add_location_flags(collection_name, config_file):
    """
    Add Spain location flags to tweets
    """
    check_current_directory()
    print('Adding location flags')
    add_esp_location_flags(collection_name, config_file)


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


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--source_collection', help='Name of source collection', \
              default=None, is_flag=False)
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--date', help='Date for which the analysis should be run', \
              default=None, is_flag=False)
def update_tweet_metrics(collection_name, source_collection, config_file, date):
    """
    Update retweet and favorite metrics of tweets
    """
    check_current_directory()
    print('Updating metrics of tweets')
    update_metric_tweets(collection_name, config_file, source_collection, date)


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def add_complete_text_flag(collection_name, config_file):
    """
    Add complete_text flag
    """
    check_current_directory()
    print('Adding complete_text flag')
    do_add_complete_text_flag(collection_name, config_file)


@run.command()
@click.argument('sample_size') # Sample size
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--output_file', help='Name of file where to save the output', \
              default=None, is_flag=False)
def extract_user_sample(sample_size, collection_name, config_file, output_file):
    """
    Extract sample of tweets and save it into a json file
    """
    check_current_directory()
    print('Extracting sample of users')
    export_user_sample(sample_size, collection_name, config_file, output_file)


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def add_tweet_type_flag(collection_name, config_file):
    """
    Add tweet type flag
    """
    check_current_directory()
    print('Adding tweet type flag')
    do_add_tweet_type_flag(collection_name, config_file)


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--user_collection_name', help='Name of the user collection', \
              default=None, is_flag=False)
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--log_file', help='Name of file to be used in logging messages', \
              default=None, is_flag=False)
def update_users_collection(collection_name, user_collection_name, config_file, 
                            log_file):
    """
    Update users collection
    """
    check_current_directory()
    print('Updating collection of users')
    while True:
        try:
            do_update_users_collection(collection_name, user_collection_name,
                                       config_file, log_file)
            break
        except (AutoReconnect, ExecutionTimeout, NetworkTimeout):
            print('Timeout exception captured, re-launching the process')


@run.command()
@click.argument('collection_name') # Name of collections that contain users
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--log_file', help='Name of file to be used in logging messages', \
              default=None, is_flag=False)
def update_user_status(collection_name, config_file, log_file):
    """
    Update status of users
    """
    check_current_directory()
    print('Updating status of users')
    do_update_user_status(collection_name, config_file, log_file)


@run.command()
@click.argument('source_collection') # Name of source collection
@click.argument('target_collection') # Name of target collection
@click.argument('start_date') # Date from when tweets should be replicated
@click.option('--end_date', help='Date until when tweets should be replicated', \
              default=None, is_flag=False)
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def replicate_tweets(source_collection, target_collection, start_date, 
                     end_date, config_file):
    """
    Replicate tweets created from start_date from source collection to target collection 
    """
    check_current_directory()
    print('Replicating tweets')
    do_tweets_replication(source_collection, target_collection, start_date, 
                          end_date, config_file)


@run.command()
@click.argument('collection_name') # Name of collections that contain users
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--log_file', help='Name of file to be used in logging messages', \
              default=None, is_flag=False)
def augment_user_data(collection_name, config_file, log_file):
    """
    Augment users' data
    """
    check_current_directory()
    print('Augmenting users\' data')
    do_augment_user_data(collection_name, config_file, log_file)


@run.command()
@click.argument('collection_name') # Name of collections that contain users
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--log_file', help='Name of file to be used in logging messages', \
              default=None, is_flag=False)
def predict_user_demographics(collection_name, config_file, log_file):
    """
    Predict users' demographics
    """
    check_current_directory()
    print('Predict users\' demographics')
    compute_user_demographics(collection_name, config_file)


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--output_file', help='Name of file where to save the output', \
              default=None, is_flag=False)
def export_users(collection_name, config_file, output_file):
    """
    Export information of users
    """
    check_current_directory()
    print('Exporting users')
    do_export_users(collection_name, config_file, output_file)


@run.command()
@click.argument('input_file') # Path to the input file
@click.option('--output_file', help='Name of the outputfile', \
              default=None, is_flag=False)
def predict_user_demographics_from_file(input_file, output_file):
    """
    Predict users' demographics from an input file
    """
    check_current_directory()
    print('Predict users\' demographics')
    compute_user_demographics_from_file(input_file, output_file)


@run.command()
@click.argument('input_file') # Path to the csv input file
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def update_user_demographics(input_file, collection_name, config_file):
    """
    Update users' demographics from a csv file
    """
    check_current_directory()
    print('Update users\' demographics')
    load_user_demographics(input_file, collection_name, config_file)


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.argument('output_file') # Path to the csv input file
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
@click.option('--stemming', help='Whether stemming should be applied to the text '\
              'of tweets before exporting them', default=False, is_flag=True)
@click.option('--lang', help='Language of tweets to be exported', default='es', 
              is_flag=False)
@click.option('--exclude_rts', help='Exclude RTs', default=False, is_flag=True)
def export_tweets(collection_name, output_file, config_file, stemming, 
                          lang, exclude_rts):
    """
    Export tweets to json
    """
    check_current_directory()
    print('Exporting tweets to json')
    export_tweets_to_json(collection_name, output_file, config_fn=config_file, 
                          stemming=stemming, lang=lang, exclude_rts=exclude_rts)

@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def create_field_created_at_date(collection_name, config_file):
    """
    Create field created_at_date based on the field created_at
    """
    check_current_directory()
    print('Creating field created_at_date')
    do_create_field_created_at_date(collection_name, config_file)


@run.command()
@click.argument('collection_name') # Name of collections that contain tweets
@click.argument('reference_date') # Date that will taken as reference for the calculation
@click.argument('time_window_in_days') # Number of days that should be considered for the analysis
@click.option('--config_file', help='File with Mongo configuration', \
              default=None, is_flag=False)
def compute_median_and_total_tweets(collection_name, reference_date, 
                                    time_window_in_days, config_file):
    """
    Compute whether the total number of tweets in above the median of tweets
    of the last X (time_window_in_days) days
    """
    check_current_directory()
    print('Computing median and total tweets')
    is_the_total_tweets_above_median(collection_name, reference_date, 
                                     time_window_in_days, config_file)


if __name__ == "__main__":
    run()