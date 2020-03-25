import pandas as pd
from utils import detect_language
from tqdm import tqdm


def infer_language(input_file_name):
    output_file_name = 'data/tweets_languages_010119to220220.csv'
    input_file_name = 'data/' + input_file_name
    tqdm.pandas()

    print('Reading file: {}'.format(input_file_name))
    tweets = pd.read_csv(input_file_name, usecols=['tweet_id', 'tweet'])
    print('Infering languages of tweets...')    
    tweets['lang'] = tweets['tweet'].progress_apply(detect_language)
    print('Saving to file: {}',format(output_file_name))
    tweets[['tweet_id', 'lang']].to_csv(output_file_name, index=False)


if __name__ == "__main__":
    infer_language('ours_2019-01-01_to_2020-02-22_coronavirus(es-en)_tweets.csv')