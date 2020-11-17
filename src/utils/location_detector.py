import csv
import emoji
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
    places, places_list = {}, []
    place_types = ['country', 'region', 'province', 'city']
    homonymous = set()
    default_place = 'unknown'
    SEPARATION_CHAR = '/'
    EMPTY_CHAR = ''
    enabled_methods = []

    def __init__(self, places_fn, flag_in_location=True, 
                 demonym_in_description=True,
                 language_of_description=True):
        self.enabled_methods = [
            {
                'parameter': 'location',
                'method_name': 'identify_place_from_location',
                'method_type': 'matching_place_location'
            }            
        ]
        if demonym_in_description:
            self.enabled_methods.append(
                {
                    'parameter': 'description',
                    'method_name': 'identify_place_from_demonyms_in_description',
                    'method_type': 'matching_demonyms_description'
                }                
            )
        if language_of_description:
            self.enabled_methods.append(
                {
                    'parameter': 'description',
                    'method_name': 'identify_place_from_description_language',
                    'method_type': 'language_description'
                }                
            )
        if flag_in_location:
            self.enabled_methods.append(
                {
                    'parameter': 'location',
                    'method_name': 'identify_place_flag_in_location',
                    'method_type': 'matching_flag_location'
                }                
            )
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
            if 'flag_emoji_code' not in self.places:
                self.places['flag_emoji_code'] = set()
            # add places's flag emoji code
            if place['flag_emoji_code'] != '':
                self.places['flag_emoji_code'].add(place['flag_emoji_code'])
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
        return

    def __load_places(self, places_fn):
        with open(places_fn, 'r') as f:
            lines = f.readlines()
            for line in lines:
                places = json.loads(line)
            self.__load_place(places)
            self.places_list = places            

    def __process_csv_row(self, row, place_type, places, homonymous):
        place_dict = None
        for place in places:                            
            if row[place_type].split(self.SEPARATION_CHAR)[0] == place['name']:
                return False, place
        place_dict = {
            'name': row[place_type].split(self.SEPARATION_CHAR)[0],
            'alternative_names': row[place_type].split(self.SEPARATION_CHAR)[1:],
            'type': place_type,
            'flag_emoji_code': row['flag_emoji_code'] if row['flag_emoji_code'] else '',
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

    def from_csv_to_json(self, places_fn, output_fn):
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
        with open(output_fn, 'w') as f:
            f.write(json.dumps(countries, ensure_ascii=False))

    def __normalize_text(self, text):
        words = remove_non_ascii(text)
        words = to_lowercase(words)
        words = remove_punctuation(words)
        words = remove_extra_spaces(words)
        return ' '.join(words)

    def __find_matching(self, places, locations):
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

    def __search_full_place(self, places, place_found, place_found_type):
        found_place = False
        dict_place = {'country': None, 'region': None, 'province': None, 
                      'city': None, 'alternative_country': [], 
                      'alternative_region': [], 'alternative_province': [],
                      'alternative_city': []}
        for place in places:
            if place['type'] == place_found_type:
                names = [place['name']]
                names.extend(place['alternative_names'])
                for name in names:
                    n_place = self.__normalize_text(name)
                    if n_place == place_found:
                        place_to_return = {place['type']: place['name']}
                        if len(place['alternative_names']) > 0:
                            key_name = 'alternative_' + place['type']
                            place_to_return[key_name] = place['alternative_names']                            
                        return True, place_to_return
            if place_found_type == 'country':
                continue
            else:
                if place['type'] == 'country' and \
                   place_found_type in ['region', 'province', 'city']:
                    found_place, dict_place = self.__search_full_place(
                        place['regions'], place_found, place_found_type)
                elif place['type'] == 'region' and \
                     place_found_type in ['province', 'city']:
                    found_place, dict_place = self.__search_full_place(
                        place['provinces'], place_found, place_found_type)
                elif place['type'] == 'province' and \
                     place_found_type == 'city':
                    found_place, dict_place = self.__search_full_place(
                        place['cities'], place_found, place_found_type)
            if found_place:
                dict_place[place['type']] = place['name']
                key_name = 'alternative_' + place['type']
                dict_place[key_name] = place['alternative_names']
                break        
        return found_place, dict_place

    def get_full_place(self, place_found, place_type):
        _, full_place = self.__search_full_place(self.places_list, place_found, 
                                              place_type)
        return full_place

    def get_place_to_return(self, full_place, place_type_found, place_to_identify):
        if place_type_found == 'country':
            place_to_identify = 'country'
        return full_place[place_to_identify]

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
                    place_found = self.__find_matching(self.places[place_type], unique_locations)
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
                        if len(normalized_location.strip()) == len(place_found):
                            # address 1)
                            place_to_return = \
                                self.get_place_to_return(full_place, place_type_found, 
                                                        place_to_identify)
                            iterate = False
                        else:
                            # address 2)
                            places_to_match = set()
                            names = [full_place['country']]                   
                            names.extend(full_place['alternative_country'])
                            for name in names:
                                n_place = self.__normalize_text(name)
                                if n_place != place_found:
                                    places_to_match.add(n_place)
                            if place_type_found == 'city' or \
                               place_type_found == 'province':
                                names = [full_place['region']]
                                names.extend(full_place['alternative_region'])                                
                                for name in names:
                                    n_place = self.__normalize_text(name)
                                    if n_place != place_found:
                                        places_to_match.add(n_place)                            
                            if place_type_found == 'city':
                                names = [full_place['province']]
                                names.extend(full_place['alternative_province'])
                                for name in names:
                                    n_place = self.__normalize_text(name)
                                    if n_place != place_found:
                                        places_to_match.add(n_place)                            
                            context_found = self.__find_matching(places_to_match, unique_locations)
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
                ret_place = self.identify_place_from_location(location)
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
                    print('Error in location: {}\nDetector answer: {} - Correct answer: {}\n'.\
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

    def __search_flag(self, places, flag_found):
        dict_place = {}
        found_flag = False
        for place in places:
            if place['flag_emoji_code'] == flag_found:
                return True, {place['type']: place['name']}
            if place['type'] == 'country':
                found_flag, dict_place = self.__search_flag(place['regions'], flag_found)
            elif place['type'] == 'region':
                found_flag, dict_place = self.__search_flag(place['provinces'], flag_found)
            elif place['type'] == 'province':
                found_flag, dict_place = self.__search_flag(place['cities'], flag_found)
            if found_flag:
                dict_place[place['type']] = place['name']
                break
        return found_flag, dict_place

    def __get_emoji_codes(self, demojized_location):
        emoji_codes = []
        in_potential_emoji_code = False
        for char in demojized_location:
            if char == ':':
                if in_potential_emoji_code:
                    in_potential_emoji_code = False
                    emoji_code += char
                    emoji_codes.append(emoji_code)
                else:
                    in_potential_emoji_code = True
                    emoji_code = char
            else:
                if in_potential_emoji_code:
                    emoji_code += char                
        return emoji_codes

    def identify_place_flag_in_location(self, location, place_to_identify='region'):
        place_to_return = self.default_place
        if location:
            demojized_location = emoji.demojize(location)
            emoji_codes = self.__get_emoji_codes(demojized_location)
            flag_found = None
            for emoji_code in emoji_codes:
                for flag_emoji in self.places['flag_emoji_code']:
                    if emoji_code.lower() == flag_emoji.lower():
                        # found a flag, now let's get the name of the place
                        flag_found = flag_emoji
                        break
                if flag_found:
                    # only the first found flag is considered
                    break
            if flag_found:
                _, flag_place = self.__search_flag(self.places_list, flag_found)
                if place_to_identify in flag_place:
                    place_to_return = flag_place[place_to_identify]
                else:
                    place_to_return = flag_place['country']
        return place_to_return

    def identify_place_from_description_language(self, location, place_to_identify='region'):
        pass

    def identify_place_from_demonyms_in_description(self, description, place_to_identify='region'):
        pass

    def identify_location(self, location, description, place_to_identify='region'):
        location_identified = self.default_place
        method_name = ''
        for enabled_method in self.enabled_methods:
            method_name = enabled_method['method_name']
            method_type = enabled_method['method_type']
            method = getattr(self, method_name)
            if 'location' == enabled_method['parameter']:
                location_identified = method(location, place_to_identify)
            elif 'description' == enabled_method['parameter']:
                location_identified = method(description, place_to_identify)
            else:
                raise Exception('Could not recognize identification method {}'.format(enabled_method))
            if location_identified != self.default_place:
                break
        return location_identified, method_type
            

if __name__ == "__main__":
    places_fn = os.path.join('data', 'places_spain.json')
    places_fn_csv = os.path.join('..','..','data', 'places_spain_new.csv')
    test_fn = os.path.join('..','..','data', 'location_detector_testset.csv')
    #config_fn = os.path.join('..', 'config_mongo_inb.json')
    #test_users = set()
    #with open(test_fn, 'r') as f:
    #        csv_reader = csv.DictReader(f)            
    #        for row in csv_reader:
    #            test_users.add(row['screen_name'])
    ld = LocationDetector(places_fn)
    #ld.from_csv_to_json(places_fn_csv, '../../data/places_spain.json')
    #ld.evaluate_detector(test_fn)
    location = '🇪🇸 madrid'
    ret_place = ld.identify_location(location, '')
    print(ret_place)

    
    



        

                
