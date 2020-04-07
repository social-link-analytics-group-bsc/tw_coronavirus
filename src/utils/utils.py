import csv
import json
import logging
import pathlib
import re
import time

from datetime import datetime, timedelta

logging.basicConfig(filename=str(pathlib.Path(__file__).parents[1].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)

SPAIN_LANGUAGES = ['es', 'ca', 'eu', 'an', 'ast', 'gl']

# Get configuration from file
def get_config(config_file):
    with open(str(config_file), 'r') as f:
        config = json.loads(f.read())
    return config


def get_tweet_datetime(tw_creation_dt):
    dt_obj = datetime.strptime(tw_creation_dt, '%a %b %d %H:%M:%S %z %Y')        
    tw_date_time = dt_obj.strftime("%d/%m/%Y, %H:%M:%S")
    tw_date = dt_obj.strftime("%d/%m/%Y")
    tw_time = dt_obj.strftime("%H:%M:%S")
    return tw_date_time, tw_date, tw_time


def calculate_remaining_execution_time(start_time, total_segs, 
                                       processing_records, total_records):
    end_time = time.time()
    total_segs += end_time - start_time
    remaining_secs = (total_segs/processing_records) * \
                     (total_records - processing_records)
    try:
        remaining_time = str(timedelta(seconds=remaining_secs))
        logging.info('Remaining execution time: {}'.format(remaining_time))
    except:
        logging.info('Remaining execution time: infinite')
    return total_segs


def get_spain_places():
    places_fn = str(pathlib.Path(__file__).parents[2].\
                joinpath('data','places_spain.csv'))
    spain_places = set()
    # Add Spain, España and Espanya as default places
    spain_places.add('spain')
    spain_places.add('españa')
    spain_places.add('espanya')
    with open(places_fn, 'r') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        for row in csv_reader:
            if row['Comunidad Autonoma'] and \
               row['Comunidad Autonoma'] not in spain_places:
                spain_places.add(row['Comunidad Autonoma'].lower())
            if row['Provincia'] and row['Provincia'] not in spain_places:
                spain_places.add(row['Provincia'].lower())
            if row['Capital'] and row['Capital'] not in spain_places:
                spain_places.add(row['Capital'].lower())
    return spain_places


def get_spain_places_regex():
    places = get_spain_places()
    places_regex_objs = []
    for place in places:
        regex_lugar = '.*{}.*'.format(place)
        places_regex_objs.append(re.compile(regex_lugar, re.IGNORECASE))
    return places_regex_objs


def get_covid_keywords():
    keywords_fn = str(pathlib.Path(__file__).parents[2].\
                      joinpath('data','keywords_covid.txt'))
    covid_keywords = []
    with open(keywords_fn, 'r') as txt_file:
        lines = txt_file.readlines()
        for line in lines:
            if line not in covid_keywords:
                covid_keywords.append(line.replace('\n',''))
    return covid_keywords