import unittest
import pathlib
import os

from utils.location_detector import LocationDetector


class testDetectorTestCase(unittest.TestCase):

    def setUp(self):
        current_path = pathlib.Path(__file__).parent.resolve()
        places_esp_fn = os.path.join(current_path, '..', 'data', 'places_spain.json')
        self.ld = LocationDetector(places_esp_fn)

    def __evaluate_test_cases(self, method_name, test_cases):
        method = getattr(self.ld, method_name)
        for test_case in test_cases:
            test = test_case['test']
            param = None
            if 'param' in test_case:
                param = test_case['param']
            if param is not None:
                ret = method(test, param)
            else:
                ret = method(test)
            self.assertEqual(ret, test_case['expected_answer'])

    def testidentify_place_flag_in_test(self):
        test_cases = [
            {
                'test': '🇵🇦, 🇪🇸, 🇩🇪 y 🇵🇪',
                'expected_answer': 'unknown'
            },
            {
                'test': '🇲🇽  🇬🇧  🇪🇸  🇦🇹 🇫🇷  🇮🇹  🇺🇸 🇳🇱 🇧🇷 🇨🇦 🇩🇪 🇧🇿',
                'expected_answer': 'unknown'
            },
            {
                'test': '✈🌍📷 🇪🇸🇬🇧🇪🇺🇮🇳🇺🇸🇨🇳',
                'expected_answer': 'unknown'
            },
            {
                'test': 'Medellín   🇪🇸🇵🇦🇩🇴',
                'expected_answer': 'unknown'
            },
            {
                'test': '🇪🇸🇪🇸',
                'expected_answer': 'España'
            },
            {
                'test': 'GC 🇮🇨',
                'expected_answer': 'Canarias'
            },
            {
                'test': '🇪🇸🇮🇹',
                'expected_answer': 'España'
            }
        ]
        self.__evaluate_test_cases('identify_place_flag_in_location', test_cases)
        


    def testidentify_place_from_demonyms_in_test(self):
        test_cases = [
            {
                'test': 'Mexicano, Michoacano, Zamorano, Atlista, Raider.  Enamorado y pareja de @claus1026. México es más que sus políticos🇲🇽',
                'param': 'Zamora, Michoacan',
                'expected_answer': 'unknown'
            },
            {
                'test': 'Hola, me gusta el color negro y el rock en español.',
                'param': 'Bucaramanga, Santander',
                'expected_answer': 'unknown'
            },
            {
                'test': 'Diputado por Lugo @GPPopular Vicesecretario Nacional de Participación 🅿️🅿️ Abogado. Gallego, por lo tanto, español. Instagram: jaimedeolanopp 👍',
                'param': '',
                'expected_answer': 'Galicia'
            },
            {
                'test': 'Texano. Me gusta lo español. Católico. Conservador.',
                'param': 'San Antonio',
                'expected_answer': 'unknown'                
            },
            {
                'test': 'SOM VALENCIANS. Pero un hombre honesto, no es frances, ni alemán, ni español, es ciudadano del mundo, y su patria esta en todas partes. 100% VALENCIANISTE.',
                'param': '',
                'expected_answer': 'Comunidad Valenciana'
            }
        ]
        self.__evaluate_test_cases('identify_place_from_demonyms_in_description', test_cases)

    def testidentify_place_from_text_in_location(self):
        test_cases = [
            {
                'test': 'Roquetes, Terres de l\'Ebre',
                'expected_answer': 'Cataluña'
            },
            {
                'test': 'Rojales, España',
                'expected_answer': 'España'
            },
            {
                'test': 'Valdemoro, España',
                'expected_answer': 'Comunidad de Madrid'
            },
            {
                'test': '23 de Enero, San Cristobal',
                'expected_answer': 'unknown'
            },
            {
                'test': 'Jesus María, Córdoba',
                'expected_answer': 'unknown'
            }
        ]
        self.__evaluate_test_cases('identify_place_from_location', test_cases)

if __name__ == '__main__':
    unittest.main()