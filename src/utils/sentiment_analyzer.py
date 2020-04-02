from classifier import SentimentClassifier
from afinn import Afinn
from polyglot.text import Text

import math

"""
Supported languages:
 - Spanish: es
 - Catalan: ca
 - Basque: eu
 - Aragones: an
 - Asturian: ast
 - Galician: gl

Polyglot needs the languages to download before used.
To download run polyglot download sentiment2.[language_code]
"""


class SentimentAnalyzer:

    supported_languages = ['es', 'ca', 'eu', 'an', 'ast', 'gl']
    sp_classifier = af_classifier = None

    def __init__(self):
        self.sp_classifier = SentimentClassifier()
        self.af_classifier = Afinn(language='es')

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
        
        For Spanish, two additional languages are used in
        the sentiment analysis. An average of
        the three analyzers are returned.
        """

        if language not in self.supported_languages:
            raise Exception('Language {} not supported! Currently supported ' \
                            'languages are: {}'.format(language, 
                                                       self.supported_languages))
        
        num_applied_analyzers = 0
        total_scores = 0.0
        sentiment_dict = {}

        # Apply Polyglot analyzer
        pg_sentiment_score = None
        pg_text = Text(text, hint_language_code=language)
        try:
            word_scores = [w.polarity for w in pg_text.words]
            pg_sentiment_score = sum(word_scores)/float(len(word_scores))
            n_pg_sentiment_score = self.normalize_score(pg_sentiment_score)
            total_scores += n_pg_sentiment_score
            num_applied_analyzers += 1
        except:
            pass
        sentiment_dict['sentiment_score_polyglot'] = pg_sentiment_score

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
        sentiment_dict['sentiment_score'] = total_scores/num_applied_analyzers
        
        return sentiment_dict
