from m3inference import M3Twitter

import csv
import json
import os
import pandas as pd
import pathlib
import pprint


def generate_m3_input():
    current_path = pathlib.Path(__file__).resolve()
    root_dir = current_path.parents[2]
    cache_dir = 'twitter_cache'
    input_file = os.path.join(root_dir, 'data', 'users_sample.jsonl')
    output_file = os.path.join(root_dir, 'data', 'm3_users_sample.jsonl')
    m3twitter = M3Twitter(cache_dir=cache_dir)
    m3twitter.transform_jsonl(input_file=input_file, output_file=output_file)


def json_to_pandas(user_objs):
    print('Converting json to pandas...')
    df = pd.DataFrame()
    for user_obj in user_objs:
        json_dict = json.loads(user_obj)
        reduced_json_dict = {
            'id': [json_dict['id']],
            'name': [json_dict['name']],
            'screen_name': [json_dict['screen_name']]
        }
        df = df.append(pd.DataFrame.from_dict(reduced_json_dict))
    return df


def infer_demographic():
    current_path = pathlib.Path(__file__).resolve()
    root_dir = current_path.parents[2]
    cache_dir = 'twitter_cache'
    input_file = os.path.join(root_dir, 'data', 'm3_users_sample.jsonl')
    output_file = os.path.join(root_dir, 'data', 'users_demo_pred.csv')
    user_objs = []
    with open(input_file) as json_file:
        json_lines = json_file.readlines()
        for json_line in json_lines:
            user_objs.append(json_line)
    user_sample_df = json_to_pandas(user_objs)
    m3twitter = M3Twitter(cache_dir=cache_dir)
    print('Running predictions...')
    predictions = m3twitter.infer(input_file)
    print('Saving tweets to the CSV {}'.format(output_file))
    with open(output_file, 'w') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=['id', 'name', 'screen_name', 'profile_link', 'age_range', 'gender', 'type'])
        csv_writer.writeheader()     
        for user_id in predictions:
            res_user = predictions[user_id]
            age_dict = sorted(res_user['age'].items(), key=lambda t: t[1], reverse=True)
            gender_dict = sorted(res_user['gender'].items(), key=lambda t: t[1], reverse=True)
            type_dict = sorted(res_user['org'].items(), key=lambda t: t[1], reverse=True)
            age_range = age_dict[0][0]
            gender = gender_dict[0][0]
            type_user = type_dict[0][0]
            user_screen_name = user_sample_df.loc[user_sample_df['id']==user_id,'screen_name'].values[0]
            user_name = user_sample_df.loc[user_sample_df['id']==user_id,'name'].values[0]
            user_profile_link = 'https://www.twitter.com/{}'.format(user_screen_name)
            row = {
                'id': user_id,
                'name': user_name,
                'screen_name': user_screen_name,
                'profile_link': user_profile_link,
                'age_range': age_range,
                'gender': gender,
                'type': type_user
            }
            csv_writer.writerow(row)

if __name__ == "__main__":
    #generate_m3_input()
    infer_demographic()