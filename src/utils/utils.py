import csv
import json
import logging
import os
import pathlib
import re
import time
import unicodedata
import urllib.request


from datetime import datetime, timedelta
from nltk.tokenize import word_tokenize
from math import ceil
from PIL import Image
from torchvision import transforms


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
    path_places = str(pathlib.Path(__file__).parents[2].\
                joinpath('data','bsc', 'spain_places'))
    spain_places = set()
    # Add Spain, España and Espanya as default places
    spain_places.add('spain')
    spain_places.add('españa')
    spain_places.add('espanya')
    expected_headers = ['comunidad autonoma', 'provincia', 'capital', 
                        'nombre_esp', 'nombre_alt', 'nombre_compuesto']
    for r, d, files in os.walk(path_places):
        for f in files:
            if '.csv' in f:
                with open(f, 'r') as csv_file:
                    csv_reader = csv.DictReader(csv_file)
                    for row in csv_reader:
                        for header in expected_headers:
                            if header in row and \
                               row[header] and \
                               row[header] not in spain_places:
                                spain_places.add(row[header].strip().lower())

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


def normalize_text(text):
    if isinstance(text,str):
        return unicodedata.normalize('NFD', text).encode('ascii', 'ignore').decode()
    else:
        return text


def remove_non_ascii(words):
    """Remove non-ASCII characters from words"""
    words = tokenize_text(words)
    new_words = []
    for word in words:
        new_word = unicodedata.normalize('NFKD', word).encode('ascii', 'ignore').decode('utf-8', 'ignore')
        new_words.append(new_word)
    return new_words


def to_lowercase(words):
    """Convert all characters of words to lowercase"""
    words = tokenize_text(words)
    new_words = []
    for word in words:
        new_word = word.lower()
        new_words.append(new_word)
    return new_words


def remove_punctuation(words):
    """Remove punctuation from words"""
    words = tokenize_text(words)
    new_words = []
    for word in words:
        new_word = re.sub(r'[^\w\s]', ' ', word)
        if new_word != '':
            new_words.append(new_word)
    return new_words


def remove_extra_spaces(words):
    words = tokenize_text(words)
    new_words = []
    for word in words:
        word_clean = ' '.join(word.split())
        new_words.append(word_clean)
    return new_words


def tokenize_text(text):
    if not isinstance(text, list):
        return word_tokenize(text)
    else:
        return text


def exists_user(user):
    """
    Check if the given user still exists
    """
    logging.info('Checking if the user {} still exists'.format(user['screen_name']))
    img_url = user['profile_image_url_https']
    try:
        _ = urllib.request.urlopen(img_url)
        return True
    except urllib.error.HTTPError as err:
        return False


def check_user_profile_image(img_path):
    img = Image.open(img_path).convert('RGB')
    if img.size[0] + img.size[1] < 400:
        raise Exception('{} is too small. Skip.'.format(img_path))
    img = img.resize((224, 224), Image.BILINEAR)
    tensor_trans = transforms.ToTensor()
    t_img = tensor_trans(img)
    img_size = t_img.size()
    if img_size[0] == 3 and img_size[1] == 224 and img_size[2] == 224:
        return True
    else:
        raise Exception('Tensor with incorrect size {}'.format(img_size))


def week_of_month(dt):
    """ Returns the week of the month for the specified date.
        Taken from 
        https://stackoverflow.com/questions/3806473/python-week-number-of-the-month
    """
    first_day = dt.replace(day=1)

    dom = dt.day
    adjusted_dom = dom + first_day.weekday()

    return int(ceil(adjusted_dom/7.0))