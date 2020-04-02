import json
import logging
import pathlib
import time

from datetime import datetime, timedelta

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[1].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)


# Get configuration from file
def get_config(config_file):
    with open(str(config_file), 'r') as f:
        config = json.loads(f.read())
    return config


def get_tweet_datetime(tw_creation_dt):
    dt_obj = datetime.strptime(tw_creation_dt, '%a %b %d %H:%M:%S %z %Y')        
    tw_date_time = dt_obj.strftime("%m/%d/%Y, %H:%M:%S")
    tw_date = dt_obj.strftime("%m/%d/%Y")
    tw_time = dt_obj.strftime("%H:%M:%S")
    return tw_date_time, tw_date, tw_time


def calculate_remaining_execution_time(start_time, total_segs, 
                                       processing_records, total_records):
    end_time = time.time()
    total_segs += end_time - start_time
    remaining_secs = (total_segs/processing_records) * \
                     (total_records - processing_records)
    remaining_time = str(timedelta(seconds=remaining_secs))
    logging.info('Time remaining to process all tweets: ' \
                 '{} to complete.'.format(remaining_time))
    return total_segs