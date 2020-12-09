import csv
import demoji
import emoji
import json
import os
import pathlib
import preprocessor as tw_preprocessor

from collections import defaultdict
from .language_detector import do_detect_language
from .utils import remove_non_ascii, to_lowercase, remove_punctuation, \
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
        
        home_dir = str(pathlib.Path.home())
        if not os.path.isdir(home_dir):
            # Download demoji codes in case
            # it doesn't exist already
            demoji.download_codes()

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
                    'parameter': 'description&location',
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
            # add places's flag emoji code
            if 'flag_emoji_code' not in self.places:
                self.places['flag_emoji_code'] = set()
            if len(place['flag_emoji_code']) > 0:
                self.places['flag_emoji_code'].update(place['flag_emoji_code'])
            # add place's languages
            if 'languages' not in self.places:
                self.places['languages'] = set()
            if len(place['languages']) > 0:
                self.places['languages'].update(place['languages'])
            # add place's demonyms
            if 'demonyms' not in self.places:
                self.places['demonyms'] = []
            if place['demonyms'] and len(place['demonyms']['names']) > 0:
                demonym_dict = {}
                for key, values in place['demonyms'].items():
                    demonym_dict[key] = []
                    for value in values:
                        demonym_dict[key].append(self.__normalize_text(value))
                self.places['demonyms'].append(demonym_dict)
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
        emoji_flags = []
        if row['flag_emoji_code'] != self.EMPTY_CHAR:
            emoji_flags = row['flag_emoji_code'].split(self.SEPARATION_CHAR)
        languages = []
        if row['language'] != self.EMPTY_CHAR:
            languages = row['language'].split(self.SEPARATION_CHAR)
        demonym_names = []
        if row['demonym'] != self.EMPTY_CHAR:
            demonym_names = row['demonym'].split(self.SEPARATION_CHAR)
        demonym_banned_prefixes = []
        if row['demonym_banned_prefixes'] != self.EMPTY_CHAR:
            demonym_banned_prefixes = row['demonym_banned_prefixes'].split(self.SEPARATION_CHAR)
        demonym_banned_places = []
        if row['demonym_banned_places'] != self.EMPTY_CHAR:
            demonym_banned_places = row['demonym_banned_places'].split(self.SEPARATION_CHAR)
        demonyms_dict = {}
        if len(demonym_names) > 0:
            demonyms_dict['names'] = demonym_names
            demonyms_dict['banned_prefixes'] = demonym_banned_prefixes
            demonyms_dict['banned_places'] = demonym_banned_places
        place_dict = {
            'name': row[place_type].split(self.SEPARATION_CHAR)[0],
            'alternative_names': row[place_type].split(self.SEPARATION_CHAR)[1:],
            'type': place_type,
            'flag_emoji_code':  emoji_flags,
            'languages': languages,
            'homonymous': 1 if row[place_type].split(self.SEPARATION_CHAR)[0] in homonymous else 0,  
            'demonyms': demonyms_dict
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
        text = text.replace('#','')
        words = remove_non_ascii(text)
        words = to_lowercase(words)
        words = remove_punctuation(words)
        words = remove_extra_spaces(words)
        return ' '.join(words)

    def __match_location(self, places, locations):
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

    def __preprocess_location(self, location):
        unique_locations = []        
        clean_location = tw_preprocessor.clean(location)
        normalized_location = self.__normalize_text(clean_location)
        locations = tokenize_text(normalized_location)        
        for location in locations:
            if location not in unique_locations:
                unique_locations.append(location)
        return unique_locations, normalized_location

    def identify_place_from_location(self, location, place_to_identify='region'):        
        place_to_return = self.default_place
        if location:
            iterate = True
            normalized_location = location
            while iterate:
                unique_locations, normalized_location = \
                    self.__preprocess_location(normalized_location)
                places_inverted_order = self.place_types.copy()
                places_inverted_order.reverse()
                place_found, place_type_found = None, None
                for place_type in places_inverted_order:
                    place_found = self.__match_location(self.places[place_type], unique_locations)
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
                            context_found = self.__match_location(places_to_match, unique_locations)
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
    
    def __print_evaluation_result(self, total, tp, tn, fp, fn):
        print('Total: {}'.format(total))
        print('TP: {}'.format(tp))
        print('TN: {}'.format(tn))
        print('FP: {}'.format(fp))
        print('FN: {}'.format(fn))
        accuracy = (tp+tn)/total
        print('Accuracy: {}'.format(round(accuracy,4)))
        precision = tp/(tp+fp)
        print('Precision: {}'.format(round(precision,4)))
        recall = tp/(tp+fn)
        print('Recall: {}'.format(round(recall,4)))
        f1 = 2*((precision*recall)/(precision+recall))
        print('F1: {}'.format(round(f1,4)))

    def evaluate_detector(self, testset_fn, errors_fn=None):
        total, tp, fp, tn, fn = 0, 0, 0, 0, 0
        total_city, tp_city, fp_city, tn_city, fn_city = 0, 0, 0, 0, 0
        total_demo, tp_demo, fp_demo, tn_demo, fn_demo = 0, 0, 0, 0, 0
        total_lang, tp_lang, fp_lang, tn_lang, fn_lang = 0, 0, 0, 0, 0
        total_flag, tp_flag, fp_flag, tn_flag, fn_flag = 0, 0, 0, 0, 0
        errors = []
        with open(testset_fn, 'r') as f:
            csv_reader = csv.DictReader(f)   
            print('Evaluating the location detector, please wait...')         
            for row in csv_reader:
                total += 1                
                location = row['location']
                description = row['description']
                testset_type = row['type']
                true_label = row['true_label']
                if testset_type == 'city':
                    total_city += 1
                    ret_place = self.identify_place_from_location(location)
                elif testset_type == 'demonyms':
                    total_demo += 1
                    ret_place = \
                        self.identify_place_from_demonyms_in_description(
                            description, location)
                elif testset_type == 'flag':
                    total_flag += 1
                    ret_place = self.identify_place_flag_in_location(location)
                elif testset_type == 'language':
                    total_lang += 1
                    ret_place = self.identify_place_from_description_language(
                        description)
                else:
                    raise Exception('Unknown test type {}'.format(testset_type))
                n_answer = true_label.lower()
                if ret_place.lower() == n_answer:
                    if ret_place.lower() != 'unknown' and n_answer != 'unknown':
                        tp += 1
                        if testset_type == 'city': tp_city += 1
                        elif testset_type == 'demonyms': tp_demo += 1
                        elif testset_type == 'language': tp_lang += 1
                        elif testset_type == 'flag': tp_flag += 1
                    else:
                        tn += 1
                        if testset_type == 'city': tn_city += 1
                        elif testset_type == 'demonyms': tn_demo += 1
                        elif testset_type == 'language': tn_lang += 1
                        elif testset_type == 'flag': tn_flag += 1
                else:
                    error_dict = {
                        'algorithm_answer': ret_place,
                        'expected_answer': true_label,
                        'location': location,
                        'description': description,
                        'testset_type': testset_type
                    }
                    if ret_place.lower() == 'unknown' and n_answer != 'unknown':
                        fn += 1
                        if testset_type == 'city': fn_city += 1
                        elif testset_type == 'demonyms': fn_demo += 1
                        elif testset_type == 'language': fn_lang += 1                            
                        elif testset_type == 'flag': fn_flag += 1
                        error_dict['type_error'] = 'false_negative'                        
                    else:
                        fp += 1
                        if testset_type == 'city': fp_city += 1
                        elif testset_type == 'demonyms': fp_demo += 1
                        elif testset_type == 'language': fp_lang += 1                            
                        elif testset_type == 'flag': fp_flag += 1
                        error_dict['type_error'] = 'false_positive'
                    errors.append(error_dict)
        # Save errors
        if errors_fn:
            print('Saving errors in provided file...')
            with open(errors_fn, 'w') as f:
                headers = list(errors[0].keys())
                csv_writer = csv.DictWriter(f, fieldnames=headers)
                csv_writer.writeheader()
                for error in errors:
                    csv_writer.writerow(error)
            print('Errors have been save here {}'.format(errors_fn))
        # Print results
        print('############# General Results ###############\n')
        self.__print_evaluation_result(total, tp, tn, fp, fn)
        print('\n')
        print('############# Test Type: City ###############\n')
        self.__print_evaluation_result(total_city, tp_city, tn_city, fp_city, fn_city)        
        print('\n')
        print('############# Test Type: Demonyms ###############\n')
        self.__print_evaluation_result(total_demo, tp_demo, tn_demo, fp_demo, fn_demo)
        print('\n')
        print('############# Test Type: Language ###############\n')
        self.__print_evaluation_result(total_lang, tp_lang, tn_lang, fp_lang, fn_lang)
        print('\n')
        print('############# Test Type: Flag ###############\n')
        self.__print_evaluation_result(total_flag, tp_flag, tn_flag, fp_flag, fn_flag)
        print('\n')

    def __search_flag(self, places, flag_found):
        dict_place = {}
        found_flag = False
        for place in places:
            for flag_emoji_code in place['flag_emoji_code']:
                if flag_emoji_code == flag_found:
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
            emoji_dict = demoji.findall(location)
            num_unknown_flags = 0
            for _, emoji_code in emoji_dict.items():
                if 'flag:' in emoji_code:
                    # Convert given code to Github format, meaning with
                    # leading and trailing colons
                    p_code = emoji_code.split(':')[1].strip().replace(' ','_')
                    p_code = ':{}:'.format(p_code)
                    if p_code not in self.places['flag_emoji_code']:
                        num_unknown_flags += 1
            if num_unknown_flags <= 1:
                # We only consider locations in which there is at maximun up 
                # to one emoji flag that is different from flags in 
                # self.places['flag_emoji_code']
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
                        if 'country' in flag_place:
                            place_to_return = flag_place['country']
        return place_to_return

    def __search_lang(self, places, lang_found, list_places):
        for place in places:
            if lang_found in place['languages']:
                list_places.append({place['type']: place['name']})
            if place['type'] == 'country':
                self.__search_lang(place['regions'], lang_found, list_places)
            elif place['type'] == 'region':
                self.__search_lang(place['provinces'], lang_found, list_places)
            elif place['type'] == 'province':
                self.__search_lang(place['cities'], lang_found, list_places)
        return list_places

    def identify_place_from_description_language(self, description, place_to_identify='region'):
        place_to_return = self.default_place
        if description:
            clean_description = tw_preprocessor.clean(description)
            normalized_description = self.__normalize_text(clean_description)
            lang_detectors = ['fasttext', 'langid', 'langdetect', 'polyglot']
            lang_detection = defaultdict(int)
            for lang_detector in lang_detectors:
                lang_detected = do_detect_language(normalized_description, lang_detector)
                lang_detection[lang_detected] += 1
            lang_detection = sorted(lang_detection.items(), key=lambda x: x[1], reverse=True)
            # three out of the four detector should be consistent with the 
            # language of the description
            maj_lang, val_maj_lang = lang_detection[0]
            if val_maj_lang >= 3:
                found_places = []
                found_places = self.__search_lang(self.places_list, maj_lang, found_places)
                found_place_types = defaultdict(list)
                for found_place in found_places:
                    place_type = list(found_place.keys())[0]
                    found_place_types[place_type].append(found_place)
                if len(found_places) > 0:
                    place_types_inverted_order = self.place_types.copy()
                    place_types_inverted_order.reverse()
                    place_dict = None
                    for place_type in place_types_inverted_order:
                        if place_type in found_place_types and \
                        len(found_place_types[place_type]) == 1:
                            place_dict = found_place_types[place_type][0]
                            break
                    if place_dict:
                        if place_to_identify in place_dict:
                            place_to_return = place_dict[place_to_identify]
                        else:
                            if 'country' in place_dict:
                                place_to_return = place_dict['country']

        return place_to_return

    def __search_demonym(self, places, demonym_found):
        dict_place = {}
        found_demonym = False
        for place in places:
            if place['demonyms']:
                for demonym in place['demonyms']['names']:
                    n_demonym = self.__normalize_text(demonym)
                    if n_demonym == demonym_found:
                        return True, {place['type']: place['name']}
            if place['type'] == 'country':
                found_demonym, dict_place = \
                    self.__search_demonym(place['regions'], demonym_found)
            elif place['type'] == 'region':
                found_demonym, dict_place = \
                    self.__search_demonym(place['provinces'], demonym_found)
            elif place['type'] == 'province':
                found_demonym, dict_place = \
                    self.__search_demonym(place['cities'], demonym_found)
            if found_demonym:
                dict_place[place['type']] = place['name']
                break
        return found_demonym, dict_place

    def __match_demonym(self, demonyms, descriptions, locations):
        matchings = set()
        for demonym in demonyms:
            for name in demonym['names']:
                demonym_length = len(name.split())
                matching_counter = 0
                demonym_found = None
                prefixes = []
                last_idx = -1
                for word in name.split():
                    try:
                        idx_word = descriptions.index(word)
                        # if it is not the first matching (i.e., last_idx==-1), 
                        # matchings should be consecutive
                        if last_idx == -1 or (last_idx+1) == idx_word:
                            matching_counter += 1                            
                        else:
                            matching_counter, prefixes =  1, []
                        last_idx = idx_word
                        if idx_word > 0:
                            prefixes.append(descriptions[idx_word-1])
                    except ValueError:
                        pass
                if matching_counter == demonym_length:
                    demonym_found = name
                    break
            if demonym_found:
                found_prefix = False
                for banned_prefix in demonym['banned_prefixes']:
                    if banned_prefix in prefixes:
                        found_prefix = True
                        break
                if found_prefix:
                    continue
                if locations:                
                    place_found = self.__match_location(demonym['banned_places'], locations)
                    if place_found:
                        continue
                matchings.add(demonym_found)                                                                        
        return matchings

    def identify_place_from_demonyms_in_description(self, description, location, 
                                                    place_to_identify='region'):
        place_to_return = self.default_place
        if description:
            clean_description = tw_preprocessor.clean(description)
            normalized_description = self.__normalize_text(clean_description)
            descriptions = tokenize_text(normalized_description)            
            unique_locations = []
            if location:
                unique_locations, _ = self.__preprocess_location(location)
            demonyms_found = self.__match_demonym(self.places['demonyms'], 
                                                  descriptions,
                                                  unique_locations)
            if len(demonyms_found) > 0:
                demonym_places = []
                for demonym_found in demonyms_found:
                    _, demonym_place = self.__search_demonym(self.places_list, demonym_found)
                    demonym_places.append(demonym_place)
                place_types_inverted_order = self.place_types.copy()
                place_types_inverted_order.reverse()
                found_place = False
                for place_type in place_types_inverted_order:
                    for demonym_place in demonym_places:
                        if place_type in demonym_place:
                            found_place = True
                            if place_to_identify in demonym_place:
                                place_to_return = demonym_place[place_to_identify]
                            else:
                                if 'country' in demonym_place:
                                    place_to_return = demonym_place['country']                
                            break
                    if found_place: break
        return place_to_return

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
            elif 'description&location' == enabled_method['parameter']:
                location_identified = method(description, location, place_to_identify)
            else:
                raise Exception('Could not recognize the identification '\
                                'method {}'.format(enabled_method))
            if location_identified != self.default_place:
                break
        return location_identified, method_type
