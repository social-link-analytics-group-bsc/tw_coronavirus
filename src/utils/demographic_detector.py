from m3inference import M3Twitter

import csv
from datetime import datetime
import json
import logging
import os
import pandas as pd
import pathlib
import pprint


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[0].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)


class DemographicDetector:

    def __init__(self, pic_dir):
        self.m3twitter = M3Twitter(cache_dir=pic_dir)
    
    def infer(self, user_objs):
        user_predictions = []
        predictions = self.m3twitter.infer(user_objs)
        for user_id in predictions:
            res_user = predictions[user_id]
            age_dict = sorted(res_user['age'].items(), key=lambda t: t[1], reverse=True)
            gender_dict = sorted(res_user['gender'].items(), key=lambda t: t[1], reverse=True)
            type_dict = sorted(res_user['org'].items(), key=lambda t: t[1], reverse=True)
            age_range = age_dict[0][0]
            gender = gender_dict[0][0]
            type_user = type_dict[0][0]
            user_predictions.append(
                {
                    'id': user_id,                
                    'age_range': age_range,
                    'gender': gender,
                    'type': type_user
                }
            )
        return user_predictions


    def infer_from_file(self, input_file, output_filename=None):
        current_path = pathlib.Path(__file__).resolve()
        root_dir = current_path.parents[2]
        if not output_filename:            
            output_filename = 'users_pred_{}.csv'.format(datetime.now().strftime('%d%m%Y_%H%M%S'))
        output_file = os.path.join(root_dir, 'data', output_filename)
        user_objs = []
        logging.info('Reading input file...')
        with open(input_file) as json_file:
            json_lines = json_file.readlines()
            for json_line in json_lines:
                user_objs.append(json_line)
        logging.info('Starting predictions...')
        predictions = self.m3twitter.infer(input_file)
        logging.info('Finished predictions')
        logging.info('Saving predictions into the file {}'.format(output_file))
        with open(output_file, 'w') as csv_file:
            csv_writer = csv.DictWriter(csv_file, fieldnames=['id', 'age_range', 'gender', 'type'])
            csv_writer.writeheader()     
            for user_id in predictions:
                res_user = predictions[user_id]
                age_dict = sorted(res_user['age'].items(), key=lambda t: t[1], reverse=True)
                gender_dict = sorted(res_user['gender'].items(), key=lambda t: t[1], reverse=True)
                type_dict = sorted(res_user['org'].items(), key=lambda t: t[1], reverse=True)
                age_range = age_dict[0][0]
                gender = gender_dict[0][0]
                type_user = type_dict[0][0]
                row = {
                    'id': user_id,
                    'age_range': age_range,
                    'gender': gender,
                    'type': type_user
                }
                csv_writer.writerow(row)