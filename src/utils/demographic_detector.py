from m3inference import M3Twitter

import csv
import json
import os
import pandas as pd
import pathlib
import pprint


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
