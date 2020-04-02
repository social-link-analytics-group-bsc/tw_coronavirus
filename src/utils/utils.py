import json

from datetime import datetime

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
