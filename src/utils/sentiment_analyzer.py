from afinn import Afinn
from classifier import SentimentClassifier
from google.cloud import translate_v2 as translate
from polyglot.downloader import downloader
from polyglot.text import Text
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

import math
import logging
import pathlib


"""
Supported languages:
 - Spanish: es
 - Catalan: ca
 - Basque: eu
 - Aragones: an
 - Asturian: ast
 - Galician: gl
 - English: en

Polyglot needs the languages to download before used.
To download run polyglot download sentiment2.[language_code]
"""


logging.basicConfig(filename=str(pathlib.Path(__file__).parents[1].joinpath('tw_coronavirus.log')),
                    level=logging.DEBUG)


class SentimentAnalyzer:

    supported_languages = ['es', 'ca', 'eu', 'an', 'ast', 'gl', 'en']
    sp_classifier = af_classifier = translator = vader_classifier = None

    def __init__(self, with_translation_support=False):
        self.sp_classifier = SentimentClassifier()
        self.af_classifier = Afinn(language='es')        
        self.vader_classifier = SentimentIntensityAnalyzer()
        self._download_polyglot_languages()
        if with_translation_support:
            self.translator = translate.Client()

    def _download_polyglot_languages(self):
        for lang in self.supported_languages:
            lang_resource = 'sentiment2.{}'.format(lang)
            if not downloader.is_installed(lang_resource):
                downloader.download('sentiment2.es')

    def normalize_score(self, score):
        # Currently the Hyperbolic Tangent Function is implemented.
        # It returns integer from -1 to 1
        return math.tanh(score)


    def analyze_sentiment(self, text, language):
        """
        Method that applies sentiment analyzers and
        normalize results to make scores between the 
        standard -1 to 1. 
        
        In general, Polyglot is used to compute the 
        sentiment score of text. 
        
        For Spanish, two additional 
        languages are used in the sentiment analysis. An 
        average of the three analyzers are returned.

        For English, vader is applied together with polyglot.
        """

        if language not in self.supported_languages:
            logging.info('Language {} not supported! Currently supported ' \
                         'languages are: {}'.format(language, self.supported_languages))
            return None
        
        sentiment_dict = {}
        num_applied_analyzers = 0
        total_scores = 0.0

        # Apply Vader analyzer
        if language == 'en':
            va_sentiment_score = self.analyze_sentiment_vader(text)
            total_scores += va_sentiment_score
            num_applied_analyzers += 1
            sentiment_dict['sentiment_score_vader'] = va_sentiment_score

        # Apply Polyglot analyzer
        pg_sentiment_score = None
        pg_text = Text(text, hint_language_code=language)
        try:
            word_scores = [w.polarity for w in pg_text.words]
            pg_sentiment_score = sum(word_scores)/float(len(word_scores))
            n_pg_sentiment_score = self.normalize_score(pg_sentiment_score)
            total_scores += n_pg_sentiment_score
            num_applied_analyzers += 1
            sentiment_dict['sentiment_score_polyglot'] = pg_sentiment_score
        except:
            pass        

        # For spanish language 
        if language == 'es':
            # Apply Sentipy analyzer
            sp_sentiment_score = self.sp_classifier.predict(text)
            sentiment_dict['sentiment_score_sentipy'] = sp_sentiment_score
            n_sp_sentiment_score = self.normalize_score(sp_sentiment_score)
            total_scores += n_sp_sentiment_score
            num_applied_analyzers += 1
            # Apply Affin analyzer
            af_sentiment_score = self.af_classifier.score(text)
            sentiment_dict['sentiment_score_affin'] = af_sentiment_score
            n_af_sentiment_score = self.normalize_score(af_sentiment_score)
            total_scores += n_af_sentiment_score
            num_applied_analyzers += 1
        # Compute final score
        if num_applied_analyzers > 0:
            sentiment_dict['sentiment_score'] = total_scores/num_applied_analyzers
        else:
            sentiment_dict['sentiment_score'] = None
        
        return sentiment_dict

    def translate_text(self, text, source_lang='es', target_lang='en'):
        translation_obj = self.translator.translate(text, 
            source_language=source_lang, 
            target_language=target_lang)
        return translation_obj['translatedText']

    def analyze_sentiment_vader(self, text, language=None, need_translation=False):
        if need_translation and language:
            text = self.translate_text(text, language)
        vader_score = self.vader_classifier.polarity_scores(text)
        return vader_score['compound']
