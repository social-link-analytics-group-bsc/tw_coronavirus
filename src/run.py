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

@click.command()
@click.option('--detect_language', help='Infer language of tweets', default=False, is_flag=True)
@click.argument('data_dir', type=click.Path(exists=True)) #Path to data directory
@click.argument('tweets_file') #Name file of the tweets datset
@click.option('--sample', help='Run task on a sample', default=False, is_flag=True)
@click.option('--create_db_users', help='Create database of users', default=False, is_flag=True)
@click.argument('tweets_db_name') #Name of the database of tweets
@click.option('--create_interaction_net', help='Create network of interactions', default=False, is_flag=True)
def run_task(detect_language, data_dir, tweets_file, sample, create_db_users, \
             tweets_db_name, create_interaction_net, users_db_name):
    if detect_language:
        infer_language(data_dir, tweets_file, sample)
    elif create_db_users:
        na = NetworkAnalyzer(tweets_db_name)
        na.create_users_db()
    elif create_interaction_net:
        na = NetworkAnalyzer()
        na.generate_network()
    else:
        click.UsageError('Illegal user: Please indicate a running option. ' \
                         'Type --help for more information of the available ' \
                         'options.')


if __name__ == '__main__':
    cd_name = os.path.basename(os.getcwd())
    if cd_name != 'src':
        click.UsageError('Illegal use: This script must run from the src directory')
    else:
        run_task()