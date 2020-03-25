from collections import defaultdict

# Language detection tools
import fasttext
import langid
from langdetect import detect
from polyglot.detect import Detector

# Load module for fasttext
model = fasttext.load_model('lib/lid.176.bin')


def detect_language(text):            
    lang_detected = defaultdict(int)

    if not text:
        raise Exception('Error!, text is empty.')

    # infer language using fasttext    
    try:
        pred_fasttext = model.predict(text, k=1)
        if pred_fasttext[1][0] >= self.threshold_confidence:
            lang_fasttext = pred_fasttext[0][0].replace('__label__','')                    
        else:
            lang_fasttext = 'undefined'
    except:
        lang_fasttext = 'undefined'
    lang_detected[lang_fasttext] += 1
    
    
    # infer language using langid
    try:
        lang_langid = langid.classify(text)[0] 
    except:
        lang_langid = 'undefined'
    lang_detected[lang_langid] += 1

    # infer language using langdetect
    try:
        lang_langdetect = detect(text)
    except:
        lang_langdetect = 'undefined'
    lang_detected[lang_langdetect] += 1

    # infer language using polyglot
    try:
        poly_detector = Detector(text, quiet=True)
        lang_polyglot = poly_detector.language.code
        # sometimes polyglot  returns the language 
        # code with an underscore, e.g., zh_Hant.
        # next, the underscore is removed
        idx_underscore = lang_polyglot.find('_')
        if idx_underscore != -1:
            lang_polyglot = lang_polyglot[:idx_underscore]
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