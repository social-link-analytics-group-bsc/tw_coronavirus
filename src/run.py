import click
import logging
import os
import pathlib
import sys

from data_wrangler import infer_language
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
@click.argument('tweets_file') #Name file of the tweets datset
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
    print('Process has finished, results were stored in the collection networks in ' \
          'your database.')


if __name__ == "__main__":
    run()