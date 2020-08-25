from utils.db_manager import DBManager
from utils.figure_maker import lineplot, bars_by_date

import os
import pandas as pd


THRESHOLD_SA = {'low': -0.1, 'high': 0.1}
# colors
BLUE_HC = '#3778BF'
LIGHT_BLUE_INT1_HC = '#539DCC'
LIGHT_BLUE_INT2_HC = '#88BEDC'
VERY_LIGHT_BLUE_HC = '#DAE8F5'
ORGANGE_HC = '#FF7F00'
GREY = 'gray'
# font sizes
X_LABELS_SIZE = 15
Y_LABELS_SIZE  = 15
X_TICKS_SIZE = 13
Y_TICKS_SIZE = 13



def get_data(collection, config_fn, dataset_filename):
    if dataset_filename and os.path.isfile(dataset_filename):
        df = pd.read_csv(dataset_filename)    
    else:
        config_fn = 'config_mongo_inb.json'
        collection = 'rc_all'
        dbm = DBManager(collection=collection, config_fn=config_fn)
        filter_query = {}
        fields_to_retrieve = {
            '_id': 0,
            'id': 1,
            'user.location':1,
            'user.screen_name': 1,
            'lang': 1,
            'sentiment.score': 1,
            'retweeted_status.id':1,
            'is_quote_status': 1,
            'in_reply_to_status_id_str': 1,
            'created_at_date': 1,
            'created_at': 1,
            'comunidad_autonoma': 1,
            'provincia': 1,
            'retweet_count': 1,
            'favorite_count': 1
        }
        data = dbm.get_tweets_reduced(filter_query,fields_to_retrieve)
        df = pd.DataFrame(data)
        data = None # free some memory
        df.to_csv(os.path.join(dataset_filename), index=False)
    
    return df


def process_data(df):
    df_columns = list(df.columns)

    if 'created_at' in df_columns:
        df['created_at'] = pd.to_datetime(df['created_at'])
    
    if 'created_at_date' in df_columns:
        df['created_at_date'] = pd.to_datetime(df['created_at_date']).dt.date
        df = df.rename(columns={'created_at_date':'date'})
    elif 'date' in df_columns:
        df['date'] = pd.to_datetime(df['date']).dt.date
    
    if 'date_hour' not in df_columns:
        df['date_hour'] = df['created_at'].dt.strftime('%Y-%m-%d %H')
    
    languages_no_to_aggregate = ['es', 'ca', 'eu', 'gl', 'en', 'pt', 'fr', 'it']
    df['lang_org'] = df['lang']
    df.loc[~df.lang.isin(languages_no_to_aggregate),'lang'] = 'otro'

    return df


def sentiment_analysis(df, img_path, save_fig_in_file=True):
    sa_df = df.loc[:, ['id', 'date', 'sentiment']]
    sa_df.loc[:, 'sentiment'] = pd.to_numeric(sa_df.loc[:, 'sentiment'])
    # Compute category sentiment category
    sa_df.loc[:, 'sentiment_label'] = np.where(
        sa_df.loc[:, 'sentiment'] > thresholds['high'], 'positivo', 
        np.where(sa_df.loc[:, 'sentiment'] < thresholds['low'], 
        'negativo', 'neutral')
    )
    # 1. Evolution of sentiment scores over time
    aesthetic_params = {
        'color': BLUE_HC,
        'marker': 'o',
        'linewidth': 0.5
    }
    fig = lineplot(sa_df, 'date', 'sentiment', 'Fecha', 'Score Sentimiento',
                   X_LABELS_SIZE, Y_LABELS_SIZE, X_TICKS_SIZE, 
                   Y_TICKS_SIZE, x_ticks_rotation=15, aesthetic_options=aesthetic_params)
    if save_fig_in_file:
        fig.get_figure().savefig(
            os.path.join(img_path, "tweets_sentiment_score_evolution.png"), 
            dpi=200, 
            quality=95, 
            bbox_inches="tight"
        )
    # 2. Evolution of sentiment categories over time (line)
    tweets_by_group = sa_df.groupby(['date', 'sentiment_label'], as_index=False)
        ['id'].count().sort_values('date', ascending=True)
    tweets_by_group.rename(
        columns={'id': 'id', 'sentiment_label': 'Sentimiento'}, 
        inplace=True
    )
    aesthetic_params = {
        'hue': tweets_by_group['Sentimiento'],
        'linewidth': 5,
        'sort': False,
        'palette':[ORGANGE_HC, GREY, BLUE_HC]
    }
    fig = lineplot(tweets_by_group, 'date', 'id', 'Fecha', 'Nro. Tweets',
                   BLUE_HC, X_LABELS_SIZE, Y_LABELS_SIZE, X_TICKS_SIZE, 
                   Y_TICKS_SIZE, x_ticks_rotation=15, aesthetic_options=aesthetic_params)
    if save_fig_in_file:
        fig.get_figure().savefig(
            os.path.join(img_path, "tweets_sentiment_category_evolution.png"), 
            dpi=200, 
            quality=95, 
            bbox_inches="tight"
    )
    # 3. Evolution of sentiment categories over time (bars)
    tweets_sentiment_by_date = tweets_by_group.pivot(columns='Sentimiento', 
        values='id', index='date')
    categories = [
        {'name': 'positivo', 'color': BLUE_HC},
        {'name': 'negativo', 'color': ORGANGE_HC},
        {'name': 'neutral', 'color': GREY}
    ]
    bars_data = []
    for category in categories:
        bars_data.append({
            'values': tweets_sentiment_by_date[category['name']],
            'color': category['color'],
            'edgecolor': 'black',
            'capsize': 7,
            'label': category['name'].title()
        })
    fig = bars_by_date(tweets_sentiment_by_date, bars_data, 'Fecha', 
                       'Nro. Tweets', X_LABELS_SIZE, Y_LABELS_SIZE,
                       X_TICKS_SIZE, Y_TICKS_SIZE, bar_width=0.3, 
                       figure_size=(15, 7))
    if save_fig_in_file:
        fig.get_figure().savefig(
            os.path.join(img_path, "tweets_sentiment_category_evolution_bars.png"), 
            dpi=200, 
            quality=95, 
            bbox_inches="tight"
        )
    # 4. Distribution of sentiment categories
    dist_sentiments = sa_df.groupby('sentiment_label', as_index=False).size().\
        to_frame(name='count').reset_index()
    dist_sentiments['prop'] = dist_sentiments['count']/sa_df.shape[0]