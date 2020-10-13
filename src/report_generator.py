from palettable.colorbrewer.sequential import Blues_4_r
from utils.db_manager import DBManager
from utils.figure_maker import lineplot, bars_by_date, donut, hlines, heatmap, \
                               barplot

import os
import pandas as pd
import numpy as np


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



def get_data(fields_to_retrieve, collection, config_fn, dataset_filename, filter_query=None):
    if dataset_filename and os.path.isfile(dataset_filename):
        df = pd.read_csv(dataset_filename)    
    else:
        config_fn = 'config_mongo_inb.json'
        collection = 'rc_all'
        dbm = DBManager(collection=collection, config_fn=config_fn)
        if not filter_query:
            filter_query = {}
        data = dbm.get_tweets_reduced(filter_query, fields_to_retrieve)
        df = pd.DataFrame(data)
        data = None # free some memory
        df.to_csv(os.path.join(dataset_filename), index=False)
    
    return df


def create_dirs(img_path, data_path):
    if not os.path.exists(img_path):
        os.mkdir(img_path)
    if not os.path.exists(data_path):
        os.mkdir(data_path)


def put_name_week_day_in_spanish(day_week):
    day, date = day_week.split()
    if day == 'Sunday':
        es_day = 'Domingo'
    elif day == 'Monday':
        es_day = 'Lunes'
    elif day == 'Tuesday':
        es_day = 'Martes'
    elif day == 'Wednesday':
        es_day = 'Miércoles'
    elif day == 'Thursday':
        es_day = 'Jueves'
    elif day == 'Friday':
        es_day = 'Viernes'
    else:
        es_day = 'Sábado'
    return es_day + ' ' + date


def pre_process_data(df):
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

    if 'day_week' not in list(df_columns):
        df['day_week'] = df['created_at'].dt.strftime('%A %d-%m-%Y')
    
    df.loc[:,'day_week'] = df.loc[:,'day_week'].apply(put_name_week_day_in_spanish)

    if 'hour' not in list(df_columns):
        df['hour'] = df['created_at'].dt.strftime('%H')    

    df.loc[df['type']=='rt', 'type'] = 'retweet'
    df.loc[df['type']=='og', 'type'] = 'original'
    df.loc[df['type']=='qt', 'type'] = 'quote'
    df.loc[df['type']=='rp', 'type'] = 'reply'

    return df


def save_figure(fig_obj, img_path, file_name, dpi=200, quality=95, 
                bbox_inches='tight'):
    fig_obj.savefig(os.path.join(img_path, file_name), dpi=dpi, quality=quality, 
        bbox_inches=bbox_inches)


def sentiment_analysis(df, img_path, save_fig_in_file=True):
    figures = []
    sa_df = df[['id', 'date', 'sentiment_score']].copy()
    sa_df.loc[:, 'sentiment_score'] = pd.to_numeric(sa_df.loc[:, 'sentiment_score'])
    # Compute category sentiment category
    sa_df.loc[:, 'sentiment_label'] = np.where(
        sa_df.loc[:, 'sentiment_score'] > THRESHOLD_SA['high'], 'positivo', 
        np.where(sa_df.loc[:, 'sentiment_score'] < THRESHOLD_SA['low'], 
        'negativo', 'neutral')
    )
    # 1. Evolution of sentiment scores over time
    aesthetic_params = {
        'color': BLUE_HC,
        'marker': 'o',
        'linewidth': 0.5
    }
    fig = lineplot(sa_df, 'date', 'sentiment_score', 'Fecha', 'Score Sentimiento',
                   X_LABELS_SIZE, Y_LABELS_SIZE, X_TICKS_SIZE, Y_TICKS_SIZE, 90, 
                   aesthetic_params)
    figures.append(fig)
    if save_fig_in_file:
        save_figure(fig.get_figure(), img_path, 'tweets_sentiment_score_evolution.png')
        
    # 2. Evolution of sentiment categories over time (line)
    tweets_by_group = sa_df.groupby(['date', 'sentiment_label'], as_index=False)\
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
                   X_LABELS_SIZE, Y_LABELS_SIZE, X_TICKS_SIZE, Y_TICKS_SIZE, 
                   90, aesthetic_params)
    figures.append(fig)
    if save_fig_in_file:
        save_figure(fig.get_figure(), img_path, 'tweets_sentiment_category_evolution.png')
        
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
                       x_ticks_rotation=90, figure_size=(15, 7))
    figures.append(fig)
    if save_fig_in_file:
        save_figure(fig, img_path, 'tweets_sentiment_category_evolution_bars.png')        

    # 4. Distribution of sentiment categories
    dist_sentiments = sa_df.groupby('sentiment_label', as_index=False).size().\
        to_frame(name='count').reset_index()
    dist_sentiments['prop'] = dist_sentiments['count']/sa_df.shape[0]
    neutral = round(dist_sentiments.loc[dist_sentiments['sentiment_label']=='neutral','prop'].values[0]*100,1)
    positive = round(dist_sentiments.loc[dist_sentiments['sentiment_label']=='positivo','prop'].values[0]*100,1)
    negative = round(dist_sentiments.loc[dist_sentiments['sentiment_label']=='negativo','prop'].values[0]*100,1)
    data = [
        {'values': neutral, 'label': 'Neutral', 'color': GREY},
        {'values': positive, 'label': 'Positivo', 'color': BLUE_HC},
        {'values': negative, 'label': 'Negativo', 'color': ORGANGE_HC}
    ]
    aesthetic_params = {
        'edgecolor': 'white',
        'linewidth': 7,
        'font_size': 14,
        'radius':1.3,
        'width': 0.5
    }
    fig = donut(data, aesthetic_params)
    figures.append(fig)
    if save_fig_in_file:
        save_figure(fig, img_path, 'tweets_sentiment_categories_donut.png')
    return figures


def ccaa_analysis(df, remove_unknown_locations, img_path, save_fig_in_file=True):
    tweets_by_group = df.groupby('comunidad_autonoma', as_index=False)['id'].count().sort_values('id', ascending=True)
    if remove_unknown_locations:
        indexes_to_drop = tweets_by_group[tweets_by_group['comunidad_autonoma']=='desconocido'].index
        tweets_by_group = tweets_by_group.drop(indexes_to_drop)
    else:
        tweets_by_group.loc[tweets_by_group['comunidad_autonoma']=='desconocido', 'comunidad_autonoma'] = 'Sin Localidad Detectada'
    fig = hlines(tweets_by_group, 'id', 'comunidad_autonoma', 'Nro. Tweets', 
                 'Comunidad Autónoma', X_LABELS_SIZE, Y_LABELS_SIZE, X_TICKS_SIZE, 
                  Y_TICKS_SIZE, LIGHT_BLUE_INT1_HC, BLUE_HC)
    if save_fig_in_file:
        save_figure(fig.get_figure(), img_path, 'tweets_locations.png')
    return [fig]


def tweet_types_analysis(df, img_path, save_fig_in_file=True):
    tweets_by_type = df.groupby('type', as_index=False)['id'].count().sort_values('id', ascending=False)
    tweets_by_type.loc[tweets_by_type['type']=='retweet', 'type'] = 'Retweets'
    tweets_by_type.loc[tweets_by_type['type']=='original', 'type'] = 'Originales'
    tweets_by_type.loc[tweets_by_type['type']=='quote', 'type'] = 'Citas'
    tweets_by_type.loc[tweets_by_type['type']=='reply', 'type'] = 'Respuestas'

    dist_types = tweets_by_type['id']/tweets_by_type['id'].sum()
    dist_types_list = list(round(100*dist_types,1))
    dist_types_list.sort(reverse=True)
    
    colors = Blues_4_r.hex_colors
    
    labels = ['Retweets', 'Originales', 'Citas', 'Respuestas']
    data = []
    for i in range(len(labels)):
        data.append(
            {
                'values': tweets_by_type.iloc[i]['id'], 
                'percentages': str(dist_types_list[i]),
                'label': labels[i],
                'color': colors[i]
            }
        )
    aesthetic_params = {
        'edgecolor': 'white',
        'linewidth': 7,
        'font_size': 14,
        'radius':1.3,
        'width': 0.5
    }
    fig = donut(data, aesthetic_params)
    if save_fig_in_file:
        save_figure(fig, img_path, 'tweets_types_donut.png')
    return [fig]


def tweets_over_time_analysis(df, img_path, save_fig_in_file=True):
    tweets_by_date = df.groupby(['date', 'type'], as_index=False)['id'].count().\
                        sort_values('date', ascending=True)
    tweets_by_date.loc[tweets_by_date['type']=='retweet', 'type'] = 'Retweet'
    tweets_by_date.loc[tweets_by_date['type']=='original', 'type'] = 'Original'
    tweets_by_date.loc[tweets_by_date['type']=='quote', 'type'] = 'Cita'
    tweets_by_date.loc[tweets_by_date['type']=='reply', 'type'] = 'Respuesta'
    tweets_by_date.rename(columns={'id': 'total', 'type': 'Tipo'}, inplace=True)
    aesthetic_params = {
        'hue': tweets_by_date['Tipo'],
        'linewidth': 3,
        'sort': False,
        'marker': 'o',
        'markersize':10,
        'palette':'Blues'
    }
    fig = lineplot(tweets_by_date, 'date', 'total', 'Fecha', 'Nro. Tweets',
                   X_LABELS_SIZE, Y_LABELS_SIZE, X_TICKS_SIZE, Y_TICKS_SIZE, 
                   90, aesthetic_params)
    if save_fig_in_file:
        save_figure(fig.get_figure(), img_path, 'tweets_types_evolution.png')
    return [fig]


def tweets_by_weekday_and_time_analysis(df, weekday_order, img_path, 
                                        save_fig_in_file=True):
    tweets_by_day_hour = df.groupby(['day_week','hour'], as_index=False)['id'].\
        count().sort_values('day_week', ascending=True)
    tweets_by_day_hour.rename(columns={'id': 'total'}, inplace=True)
    tweets_by_day_hour = tweets_by_day_hour.pivot('day_week','hour','total')
    reindex_order = [None]*7
    for idx in tweets_by_day_hour.index:
        for wd_idx, week_day in enumerate(weekday_order):
            if week_day in idx:
                reindex_order[wd_idx] = idx
    tweets_by_day_hour = tweets_by_day_hour.reindex(reindex_order)
    fig = heatmap(tweets_by_day_hour, 'Hora', 'Día de semana', X_LABELS_SIZE, 
                  Y_LABELS_SIZE, X_TICKS_SIZE, Y_TICKS_SIZE, 0.5, 'Blues')
    if save_fig_in_file:
        save_figure(fig.get_figure(), img_path, 'tweets_weekday_hours.png')
    return [fig]


def tweets_sentiment_categories_by_weekday_and_time_analysis(df, weekday_order, 
                                                             img_path, 
                                                             save_fig_in_file=True):
    sentiments_by_day_hour = df.groupby(['day_week','hour'])['sentiment_score'].\
        mean().reset_index().sort_values('day_week', ascending=True)
    sentiments_by_day_hour = sentiments_by_day_hour.pivot('day_week','hour',
                                                          'sentiment_score')
    reindex_order = [None]*7
    for idx in sentiments_by_day_hour.index:
        for wd_idx, week_day in enumerate(weekday_order):
            if week_day in idx:
                reindex_order[wd_idx] = idx
    sentiments_by_day_hour = sentiments_by_day_hour.reindex(reindex_order)
    fig = heatmap(sentiments_by_day_hour, 'Hora', 'Día de semana', X_LABELS_SIZE, 
                  Y_LABELS_SIZE, X_TICKS_SIZE, Y_TICKS_SIZE, 0.5, 'RdBu', 
                  {'vmin': -0.09, 'vmax': 0.01})
    if save_fig_in_file:
        save_figure(fig.get_figure(), img_path, 'tweets_weekday_hours.png')
    return [fig]


def unique_users_over_time_analysis(df, img_path, save_fig_in_file=True):
    users_by_date = df.groupby(['date'])['user_screen_name'].nunique().\
        reset_index().sort_values('date', ascending=True)
    users_by_date.rename(columns={'user_screen_name': 'unique_users'}, inplace=True)
    fig = barplot(users_by_date, 'date', 'unique_users', 'Fecha', 'Usuarios Únicos', 
                  X_LABELS_SIZE, Y_LABELS_SIZE, X_TICKS_SIZE, Y_TICKS_SIZE, 90, BLUE_HC)
    if save_fig_in_file:
        save_figure(fig.get_figure(), img_path, 'users_date.png')
    return [fig]


def retweet_impact_analysis(collection, config_fn):
    dbm = DBManager(collection=collection, config_fn=config_fn)
    filter_query = {
        'retweeted_status': {'$exists': 1}, # it must be a retweet
        'in_reply_to_status_id_str': {'$eq': None}, # it must not be a reply
        'is_quote_status': False # it must not be a quote
    }
    fields_to_retrieve = {
        '_id': 0,
        'user.screen_name': 1,
        'retweeted_status.id': 1,
        'retweeted_status.user.screen_name': 1
    }
    tweets = list(dbm.find_all(filter_query, fields_to_retrieve))
    df = pd.DataFrame()
    for tweet in tweets:
        df = df.append({
            'user_screen_name': tweet['user']['screen_name'],
            'retweeted_status_id': tweet['retweeted_status']['id'],
            'retweeted_status_user_screen_name': tweet['retweeted_status']['user']['screen_name'],
        }, ignore_index=True)

    d_retweeted_tweets = df.groupby(['retweeted_status_user_screen_name'])['retweeted_status_id'].nunique().to_dict()
    d_retweeting_users = df.groupby(['retweeted_status_user_screen_name'])['user_screen_name'].nunique().to_dict()

    ri_df = pd.DataFrame()
    ri_df['retweeted_user_screen_name'] = df['retweeted_status_user_screen_name']
    ri_df['retweeted_tweets'] = df.retweeted_status_user_screen_name.map(d_retweeted_tweets)
    ri_df['retweeting_users'] = df.retweeted_status_user_screen_name.map(d_retweeting_users)
    ri_df['retweet_impact'] = ri_df['retweeted_tweets'] * np.log(ri_df['retweeting_users'])
    ri_df = ri_df.sort_values(by=['retweet_impact'],ascending=False).drop_duplicates()
    ri_df['retweet_impact'] = np.log10(ri_df['retweet_impact'])
    ri_df = ri_df.replace([np.inf, -np.inf], np.nan).dropna()
    
    return ri_df


def generate_html(output_filename, content):
    html = '<!DOCTYPE html>\n'
    html += '<html>\n'
    html += '<head>\n'
    html += '<title>{}</title>\n'.format(content['title'])
    html += '</head>\n'
    html += '<body>\n'
    html += '<h1>{}</h1>'.format(content['title'])
    if 'subtitle' in content:
        html += '<h3>{}</h3>'.format(content['subtitle'])
    html += '<hr>'
    for analysis in content['analyses']:        
        html += '<h2>{}</h2>\n'.format(analysis['title'])
        if 'text' in analysis:
            html += '<p>{}</p>\n'.format(analysis['text'])
        if 'figure' in analysis:
            html += '<img src="{0}" alt="{1}" width="{2}" height="{3}">\n'.\
                format(analysis['figure']['path'], analysis['figure']['name'],
                       analysis['figure']['width'], analysis['figure']['height'])
        if 'comment' in analysis:
            html += '<p>{}</p>\n'.format(analysis['comment'])
        html += '<hr>'
    html += '</body>\n'
    html += '</html>'
    html_file = open(output_filename, 'w')
    html_file.write(html)
    html_file.close()


if __name__ == "__main__":
    # define constants
    collection_name = 'rc_all'
    mongo_config_fn = 'config_mongo_inb.json'
    report_name = 'radar_26082020'
    report_dir = os.path.join('..','reports',report_name) 
    data_path = os.path.join(report_dir, 'data')
    img_path = os.path.join(report_dir, 'figures')
    output_filename = os.path.join(report_dir, 'reporte_'+report_name+'.html')
    dataset_file_name = os.path.join(data_path,'dataset_reporte_'+report_name+'.csv')
    output = {
        'title': 'Reporte Tweets sobre RadarCovid',      
        'analyses': []
    }

    print('[0] Creating directories...')
    create_dirs(img_path, data_path)
    print('[1] Getting data...')
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
        'favorite_count': 1,
        'type': 1
    }
    df = get_data(fields_to_retrieve, collection_name, mongo_config_fn, dataset_file_name)
    output['subtitle'] = 'Período: {0} - {1}<br>Total de tweets: {2:,} - Total usuarios únicos: {3:,}'\
        .format('14-08-2020', '24-08-2020', df.shape[0], df.groupby('user_screen_name').ngroups)
    
    print('[2] Pre-processing data...')
    df = pre_process_data(df)

    print('[3] Analyzing evolution of tweets over time...')
    tweets_over_time_analysis(df, img_path)
    output['analyses'].append(
        {
            'title': 'Distribución de tweets por fecha',
            'figure': {
                'path': os.path.join('figures', 'tweets_types_evolution.png'),
                'name': 'distribucion_tweets_fecha',
                'height': '50%',
                'width': '50%'
            }
        }
    )

    print('[4] Analyzing tweet types...')
    tweet_types_analysis(df, img_path)
    output['analyses'].append(
        {
            'title': 'Distribución de tweets por tipo',
            'figure': {
                'path': os.path.join('figures', 'tweets_types_donut.png'),
                'name': 'distribucion_tweets_tipo',
                'height': '40%',
                'width': '40%'
            }            
        }
    )

    print('[5] Analyzing autonomous communities...')
    ccaa_analysis(df, True, img_path)
    output['analyses'].append(
        {
            'title': 'Distribución de tweets por Comunidad Autónoma',
            'figure': {
                'path': os.path.join('figures', 'tweets_locations.png'),
                'name': 'distribucion_tweets_ccaa',
                'height': '50%',
                'width': '50%'
            },
            'comment': 'Tweets con localidad desconocida fueron excluídos del análisis'
        }
    )

    print('[6] Analyzing sentiment of tweets...')
    sentiment_analysis(df, img_path)
    output['analyses'].append(
        {
            'title': 'Distribución de polaridad de tweets',
            'figure': {
                'path': os.path.join('figures', 'tweets_sentiment_categories_donut.png'),
                'name': 'distribucion_tweets_polaridad',
                'height': '30%',
                'width': '30%'
            }
        }
    )
    output['analyses'].append(
        {
            'title': 'Evolución de polaridad de tweets por fecha',
            'figure': {
                'path': os.path.join('figures', 'tweets_sentiment_category_evolution_bars.png'),
                'name': 'distribucion_tweets_polaridad_fecha',
                'height': '50%',
                'width': '50%'
            }
        }
    )    

    print('[7] Analyzing distribution of unique users...')
    unique_users_over_time_analysis(df, img_path)

    print('[8] Analyzing distribution of tweets by weekday and time...')
    weekdays_order = ['Martes','Miércoles','Jueves','Viernes','Sábado','Domingo','Lunes']
    tweets_by_weekday_and_time_analysis(df, weekdays_order, img_path)

    print('[9] Analyzing distribution of tweets sentiment categories by weekday and time...')
    tweets_sentiment_categories_by_weekday_and_time_analysis(df, weekdays_order, 
                                                             img_path)

    print('[10] Computing retweet impact...')
    ri_df = retweet_impact_analysis(collection_name, mongo_config_fn)
    ri_df.to_csv(os.path.join(report_dir, 'retweet_impact.csv'), index=None)

    print('[11] Generating output...')
    generate_html(output_filename, output)
