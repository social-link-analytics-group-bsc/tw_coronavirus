from collections import defaultdict

# Language detection tools
import fasttext
from langdetect import detect_langs
from polyglot.detect import Detector
from langid.langid import LanguageIdentifier, model

# Load module for fasttext
ft_model = fasttext.load_model('lib/lid.176.bin')

# Instiantiate a langid language identifier object
langid_identifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)


def detect_language(text):
    threshold_confidence = 0.75    
    lang_detected = defaultdict(int)

    if not text:
        raise Exception('Error!, text is empty.')

    # infer language using fasttext    
    try:
        pred_fasttext = ft_model.predict(text, k=1)
        if pred_fasttext[1][0] >= threshold_confidence:
            lang_fasttext = pred_fasttext[0][0].replace('__label__','')                    
        else:
            lang_fasttext = 'undefined'
    except:
        lang_fasttext = 'undefined'
    lang_detected[lang_fasttext] += 1
    
    
    # infer language using langid
    try:
        pred_langid = langid_identifier.classify(text)
        if pred_langid[1] >= threshold_confidence:
            lang_langid = pred_langid[0]
        else:
            lang_langid = 'undefined' 
    except:
        lang_langid = 'undefined'
    lang_detected[lang_langid] += 1

    # infer language using langdetect
    try:
        pred_langdetect = detect_langs(text)[0]
        lang_langdetect, conf_langdetect = str(pred_langdetect).split(':')
        conf_langdetect = float(conf_langdetect)
        if conf_langdetect < threshold_confidence:
            lang_langdetect = 'undefined'
    except:
        lang_langdetect = 'undefined'
    lang_detected[lang_langdetect] += 1

    # infer language using polyglot
    try:
        poly_detector = Detector(text, quiet=True)
        lang_polyglot = poly_detector.language.code
        conf_polyglot = poly_detector.language.confidence/100
        if conf_polyglot >= threshold_confidence:
            # sometimes polyglot  returns the language 
            # code with an underscore, e.g., zh_Hant.
            # next, the underscore is removed
            idx_underscore = lang_polyglot.find('_')
            if idx_underscore != -1:
                lang_polyglot = lang_polyglot[:idx_underscore]
        else:
            lang_polyglot = 'undefined'
    except:
        lang_polyglot = 'undefined'
    lang_detected[lang_polyglot] += 1

    # choose language with the highest counter
    max_counter, pref_lang = -1, ''
    for lang, counter in lang_detected.items():
        if lang == 'undefined':
            continue
        if counter > max_counter:
            pref_lang = lang
            max_counter = counter
        elif counter == max_counter:
            pref_lang += '_' + lang
    
    return pref_lang if pref_lang != '' else 'undefined'