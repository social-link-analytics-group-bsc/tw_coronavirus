import csv
import json
import os
import pathlib
import preprocessor as tw_preprocessor

from db_manager import DBManager
from utils import remove_non_ascii, to_lowercase, remove_punctuation, \
                  remove_extra_spaces, tokenize_text

tw_preprocessor.set_options(tw_preprocessor.OPT.URL, 
                            tw_preprocessor.OPT.MENTION,
                            tw_preprocessor.OPT.RESERVED,
                            tw_preprocessor.OPT.NUMBER,
                            tw_preprocessor.OPT.EMOJI)


class LocationDetector:
    csv_headers = ['country', 'region', 'province', 'city', 'homonymous_places', 
                   'language', 'flag_emoji_shortcode']
    places = {}
    places_list = []
    place_types = ['country', 'region', 'province', 'city']
    homonymous = set()
    default_place = 'unknown'
    SEPARATION_CHAR = '/'
    EMPTY_CHAR = ''

    def __init__(self, places_fn):        
        self.__load_places(places_fn)
    
    def __load_place(self, places):
        for place in places:                
            names = [place['name']]
            names.extend(place['alternative_names'])
            if place['type'] not in self.places:
                self.places[place['type']] = set()
            # load names
            for name in names:
                n_place = self.__normalize_text(name)
                self.places[place['type']].add(n_place)
            if 'flag_emoji_shortcode' not in self.places:
                self.places['flag_emoji_shortcode'] = set()
            # add places's flag emoji code
            self.places['flag_emoji_shortcode'].add(place['flag_emoji_shortcode'])
            if 'languages' not in self.places:
                self.places['languages'] = set()
            # add place's languages
            for language in place['languages']:
                self.places['languages'].add(language)
            if place['homonymous'] == 1:
                self.homonymous.add(self.__normalize_text(place['name']))
            if place['type'] == 'country':
                self.__load_place(place['regions'])
            elif place['type'] == 'region':
                self.__load_place(place['provinces'])
            elif place['type'] == 'province':
                self.__load_place(place['cities'])
            else:
                return

    def __load_places(self, places_fn):
        with open(places_fn, 'r') as f:
            lines = f.readlines()
            for line in lines:
                places = json.loads(line)
            self.__load_place(places)
            self.places_list = places
            # csv_reader = csv.DictReader(f)            
            # for row in csv_reader:
            #     for header in self.csv_headers:
            #         if header not in self.places:
            #             self.places[header] = set()
            #         if header in self.place_types:
            #             if row[header] != self.EMPTY_CHAR:
            #                 for place in row[header].split(self.SEPARATION_CHAR):
            #                     n_place = self.__normalize_text(place)
            #                     self.places[header].add(n_place)                            
            #         else:                        
            #             if header == 'language':
            #                 for language in row[header].split(self.SEPARATION_CHAR):
            #                     self.places[header].add(language)
            #             elif header == 'homonymous_places':
            #                 if row[header] != '':
            #                     for place in row[header].split(self.SEPARATION_CHAR):
            #                         n_place = self.__normalize_text(place)
            #                         self.homonymous.add(n_place)
            #             else:
            #                 self.places[header].add(row[header])                    
            #     self.places_list.append(row)

    def __process_csv_row(self, row, place_type, places, homonymous):
        place_dict = None
        for place in places:                            
            if row[place_type].split(self.SEPARATION_CHAR)[0] == place['name']:
                return False, place
        place_dict = {
            'name': row[place_type].split(self.SEPARATION_CHAR)[0],
            'alternative_names': row[place_type].split(self.SEPARATION_CHAR)[1:],
            'type': place_type,
            'flag_emoji_shortcode': row['flag_emoji_shortcode'] if row['flag_emoji_shortcode'] else '',
            'languages': row['language'].split(self.SEPARATION_CHAR),
            'homonymous': 1 if row[place_type].split(self.SEPARATION_CHAR)[0] in homonymous else 0,                
        }                    
        if place_type == 'country':
            place_dict['regions'] = []
        elif place_type == 'region':
            place_dict['provinces'] = []
        elif place_type == 'province':
            place_dict['cities'] = []        
        return True, place_dict

    def from_csv_to_json(self, places_fn):
        countries = []
        homonymous = set()
        with open(places_fn, 'r') as f:
            csv_reader = csv.DictReader(f)            
            for row in csv_reader:
                if row['homonymous_places'] != self.EMPTY_CHAR:
                    homonymous.add(row['homonymous_places'])
                new_place, country_dict = self.__process_csv_row(row, 'country', countries, homonymous)
                if new_place:
                    countries.append(country_dict)
                if row['region'] != self.EMPTY_CHAR:
                    new_place, region_dict = self.__process_csv_row(row, 'region', country_dict['regions'], homonymous)
                    if new_place:                        
                        country_dict['regions'].append(region_dict)
                    if row['province'] != self.EMPTY_CHAR:
                        new_place, province_dict = self.__process_csv_row(row, 'province', region_dict['provinces'], homonymous)
                        if new_place:                            
                            region_dict['provinces'].append(province_dict)
                        if row['city'] != self.EMPTY_CHAR:
                            new_place, city_dict = self.__process_csv_row(row, 'city', province_dict['cities'], homonymous)
                            if new_place:                            
                                province_dict['cities'].append(city_dict)
        with open('data/places_spain.json', 'w') as f:
            f.write(json.dumps(countries, ensure_ascii=False))
        print('Finished!')

    def __normalize_text(self, text):
        words = remove_non_ascii(text)
        words = to_lowercase(words)
        words = remove_punctuation(words)
        words = remove_extra_spaces(words)
        return ' '.join(words)

    def find_matching(self, places, locations):
        matchings = []
        matching_place = None
        for place in places:
            name_length = len(place.split())
            matching_counter = 0
            for name in place.split():
                if name in locations:
                    matching_counter += 1
            if matching_counter == name_length:
                # found a matching
                # if the found matching is contained
                # in a name already present in 
                # matchings, the name in matchings
                # is removed.
                # ------ example ------ 
                # matchings = ['san sebastian']
                # place = 'san sebastian de los reyes'
                # 'san sebastian' in matchings is removed
                # before adding 'san sebastian de los 
                # reyes'
                for matching in matchings:
                    if matching in place:
                        matchings.remove(matching)
                matchings.append(place)
        # return the place that matches with
        # the first location from left to right
        # in case there is match with more than
        # one location
        min_idx = 1000000
        for matching in matchings:
            try:
                current_idx = locations.index(matching.split()[0])
                if current_idx < min_idx:
                    min_idx = current_idx
                    matching_place = matching
            except ValueError:
                pass
        return matching_place

    def get_full_place(self, place_found, place_type):
        for place in self.places_list:
            for subplace in place[place_type].split(self.SEPARATION_CHAR):
                n_place = self.__normalize_text(subplace)
                if n_place == place_found:
                    return place
        return None

    def get_place_to_return(self, full_place, place_type_found, place_to_identify):
        if place_type_found == 'country':
            place_to_identify = 'country'
        place = full_place[place_to_identify]
        return place.split(self.SEPARATION_CHAR)[0]

    def identify_place_from_location(self, location, place_to_identify='region'):        
        place_to_return = self.default_place
        if location:
            clean_location = tw_preprocessor.clean(location)
            normalized_location = self.__normalize_text(clean_location)
            iterate = True
            while iterate:
                locations = tokenize_text(normalized_location)
                unique_locations = []
                for location in locations:
                    if location not in unique_locations:
                        unique_locations.append(location)
                places_inverted_order = self.place_types.copy()
                places_inverted_order.reverse()
                place_found, place_type_found = None, None
                for place_type in places_inverted_order:
                    place_found = self.find_matching(self.places[place_type], unique_locations)
                    if place_found:
                        place_type_found = place_type
                        break
                if place_found:
                    full_place = self.get_full_place(place_found, place_type_found)
                    if place_found in self.homonymous:
                        # if a place has homonymous, it will consider if either
                        # 1) the place is the only name in location or
                        # 2) location has also the name of region, province, or 
                        # country where the place is located
                        if len(normalized_location) == len(place_found):
                            # address 1)
                            place_to_return = \
                                self.get_place_to_return(full_place, place_type_found, 
                                                        place_to_identify)
                            iterate = False
                        else:
                            # address 2)
                            places_to_match = set()
                            for place in full_place['country'].split(self.SEPARATION_CHAR):
                                n_place = self.__normalize_text(place)
                                if n_place != place_found:
                                    places_to_match.add(n_place)
                            if place_type_found == 'city' or \
                            place_type_found == 'province':
                                for place in full_place['region'].split(self.SEPARATION_CHAR):
                                    n_place = self.__normalize_text(place)
                                    if n_place != place_found:
                                        places_to_match.add(n_place)                            
                            if place_type_found == 'city':
                                for place in full_place['province'].split(self.SEPARATION_CHAR):
                                    n_place = self.__normalize_text(place)
                                    if n_place != place_found:
                                        places_to_match.add(n_place)                            
                            context_found = self.find_matching(places_to_match, unique_locations)
                            if context_found:
                                place_to_return = self.get_place_to_return(full_place, 
                                                                        place_type_found, 
                                                                        place_to_identify)
                                iterate = False
                            else:
                                # when dealing with an homonymous place and information
                                # about its context (region, provice, country) cannot
                                # be found, we remove it from location and iterate again
                                # to see whether there are in locations another place
                                # that can be identified
                                normalized_location = normalized_location.replace(place_found, '')                                
                    else:
                        place_to_return = self.get_place_to_return(full_place, 
                                                                place_type_found, 
                                                                place_to_identify)
                        iterate = False
                else:
                    iterate = False
        
        return place_to_return
            
    def evaluate_detector(self, testset_fn):
        total, tp, fp, tn, fn = 0, 0, 0, 0, 0
        with open(testset_fn, 'r') as f:
            csv_reader = csv.DictReader(f)            
            for row in csv_reader:
                total += 1
                error = False           
                location = row['location']
                ret_place = ld.identify_place_from_location(location)
                n_answer = row['correct_location'].strip().lower()
                if ret_place.lower() == n_answer:
                    if ret_place.lower() != 'unknown' and n_answer != 'unknown':
                        tp += 1
                    else:
                        tn += 1
                else:
                    error = True
                    if ret_place.lower() == 'unknown' and n_answer != 'unknown':
                        fn += 1
                    elif ret_place.lower() != 'unknown' and n_answer == 'unknown':
                        fp += 1
                    else:                
                        fp += 1
                if error:
                    print('Error in location: {}\Detector answer: {} - Correct answer: {}\n'.\
                          format(row['location'], ret_place, row['correct_location']))
        # Print results
        print('Test set size: {}'.format(total))
        print('TP: {}'.format(tp))
        print('TN: {}'.format(tn))
        print('FP: {}'.format(fp))
        print('FN: {}'.format(fn))
        print('Accuracy: {}'.format(round((tp+tn)/total,4)))
        print('Precision: {}'.format(round(tp/(tp+fp),4)))
        print('Recall: {}'.format(round(tp/(tp+fn),4)))

    def generate_sample(self, banned_users, output_fn, sample_size=3000, 
                        config_fn=None):
        dbm_users = DBManager(collection='users', config_fn=config_fn)
        projection = {
            '_id': 0,
            'id_str': 1,
            'screen_name': 1,
            'location': 1
        }
        users = list(dbm_users.find_all({}, projection))
        processed_counter = 0
        with open(output_fn, 'w') as f:
            headers = ['id_str', 'screen_name', 'location', 'comunidad_autonoma']
            csv_writer = csv.DictWriter(f, fieldnames=headers)
            csv_writer.writeheader()
            for user in users:
                if user['screen_name'] not in banned_users and \
                processed_counter < sample_size:
                    print('[{0}] Processing the location of the user: {1}'.format(processed_counter, user['screen_name']))
                    processed_counter += 1
                    location = user['location']
                    ret_place = ld.identify_place_from_location(location)
                    user['comunidad_autonoma'] = ret_place if ret_place != 'unknown' else 'no determinado'
                    csv_writer.writerow(user)
                if processed_counter == sample_size:
                    break

if __name__ == "__main__":
    places_fn = os.path.join('data', 'places_spain.json')    
    #test_fn = os.path.join('..','..', 'data', 'location_detector_testset.csv')
    #config_fn = os.path.join('..', 'config_mongo_inb.json')
    #test_users = set()
    #with open(test_fn, 'r') as f:
    #        csv_reader = csv.DictReader(f)            
    #        for row in csv_reader:
    #            test_users.add(row['screen_name'])
    ld = LocationDetector(places_fn)
    print('Finized loading!')

    
    



        

                
