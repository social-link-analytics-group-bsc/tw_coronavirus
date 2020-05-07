# -*- coding: utf-8 -*-

import copy
import dash
import dash_core_components as dcc
import dash_html_components as html
import datetime
import json
import numpy as np
import pandas as pd
import pathlib
import plotly.express as px
import plotly.graph_objects as go

from dash.dependencies import Input, Output, ClientsideFunction
from urllib.request import urlopen


# get relative data folder
ROOT_PATH = pathlib.Path(pathlib.Path(__file__).resolve()).parents[2]
DATA_PATH = ROOT_PATH.joinpath("reports","5_2304290420","data", "dataset_reporte_5_2304290420.csv").resolve()


app = dash.Dash(
    __name__,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)


server = app.server
app.config.suppress_callback_exceptions = True


df = pd.read_csv(DATA_PATH)


dates_range_list = [
    '24-03-2020 a 02-04-2020',
    '03-04-2020 a 07-04-2020',
    '08-04-2020 a 16-04-2020',
    '17-04-2020 a 22-04-2020',
    '24-04-2020 a 29-04-2020'
]


# Create global chart template
mapbox_access_token = "pk.eyJ1IjoicGxvdGx5bWFwYm94IiwiYSI6ImNrOWJqb2F4djBnMjEzbG50amg0dnJieG4ifQ.Zme1-Uzoi75IaFbieBDl3A"


layout = dict(
    autosize=True,
    margin=dict(l=30, r=30, b=20, t=40),
    hovermode="closest",
    plot_bgcolor="#F9F9F9",
    paper_bgcolor="#F9F9F9",
    legend=dict(font=dict(size=10), orientation="h"),
    title="",
    mapbox=dict(
        accesstoken=mapbox_access_token,
        style="light",
        center=dict(lon=-78.05, lat=42.54),
        zoom=7,
    ),
)


def generate_menu_bar():
    """
    :return: A Div containing a menu bar with title & descriptions.
    """
    return html.Div(
        id="menu_bar",
        children=[
            html.H5("BSC Tweets Analytics"),
            html.H3("Dashboard Análitico sobre Tweets relacionados al COVID19 en España"),
            html.Div(
                id="intro",
                children="Explore los datos más relevantes de los tweets sobre el COVID19 publicados en España. Para ello elija algún rango de fechas de interés utilizando el control de más abajo",
            ),
        ],
    )

def generate_controls():
    """
    :return: A Div containing controls for graphs.
    """
    return html.Div(
        id="control-card",
        children=[
            html.P(html.B("Seleccione Rango de Fecha")),
            dcc.Dropdown(
                id="dates_range_select",
                options=[{"label": d, "value": d} for d in dates_range_list],
                value=dates_range_list[0],
                multi=True
            ),          
        ],
    )


def prepare_sentiment_df():
    thresholds = {'low': -0.1, 'high': 0.1}
    sa_df = df.loc[:, ['id', 'date', 'sentiment']]
    sa_df.loc[:, 'sentiment'] = pd.to_numeric(sa_df.loc[:, 'sentiment'])
    sa_df.loc[:, 'sentiment_label'] = np.where(sa_df.loc[:, 'sentiment'] > thresholds['high'], 'positivo', np.where(sa_df.loc[:, 'sentiment'] < thresholds['low'], 'negativo', 'neutral'))
    return sa_df


def get_sentiment_colors():
    sentiment_colors = {
        'negativo': '#FF7F00',
        'positivo': '#3778BF',
        'neutral': '#CCCCCC'
    }
    return sentiment_colors


@app.callback(Output("tweets", "children"), [Input("dates_range_select", "value")])
def update_num_tweets(dates_range_select):    
    return '{:,}'.format(df.shape[0])


@app.callback(Output("users", "children"), [Input("dates_range_select", "value")])
def update_num_users(dates_range_select):    
    return '{:,}'.format(df.groupby('user_screen_name').ngroups)


@app.callback(Output("rts", "children"), [Input("dates_range_select", "value")])
def update_num_rts(dates_range_select):
    tweets_types = df.groupby('type', as_index=False)['id'].count()
    num_rts = tweets_types.loc[tweets_types['type']=='rt','id'].values[0]
    return '{:,}'.format(num_rts)


@app.callback(Output("ogs", "children"),[Input("dates_range_select", "value")])
def update_num_ogs(dates_range_select):
    tweets_types = df.groupby('type', as_index=False)['id'].count()
    num_ogs = tweets_types.loc[tweets_types['type']=='og','id'].values[0]
    return '{:,}'.format(num_ogs)


@app.callback(Output("rps", "children"),[Input("dates_range_select", "value")])
def update_num_rps(dates_range_select):
    tweets_types = df.groupby('type', as_index=False)['id'].count()
    num_rps = tweets_types.loc[tweets_types['type']=='rp','id'].values[0]
    return '{:,}'.format(num_rps)


@app.callback(Output("qts", "children"),[Input("dates_range_select", "value")])
def update_num_qts(dates_range_select):
    tweets_types = df.groupby('type', as_index=False)['id'].count()
    num_qts = tweets_types.loc[tweets_types['type']=='qt','id'].values[0]
    return '{:,}'.format(num_qts)


def create_tweets_evolution_figure():
    current_layout = copy.deepcopy(layout)

    tweets_by_date = df.groupby(['date_hour', 'type'], as_index=False)['id'].count().\
                     sort_values('date_hour', ascending=True)

    index = tweets_by_date['date_hour'].unique()
    rts = tweets_by_date.loc[tweets_by_date['type']=='rt','id'].values
    ogs = tweets_by_date.loc[tweets_by_date['type']=='og','id'].values
    rps = tweets_by_date.loc[tweets_by_date['type']=='rp','id'].values
    qts = tweets_by_date.loc[tweets_by_date['type']=='qt','id'].values

    color_scale = px.colors.sequential.Blues_r

    xaxis_labels = []
    for idx in index:
        date, hour = idx.split()
        if hour in ['06', '12', '18']:
            xaxis_labels.append(hour)
        elif hour == '00':
            xaxis_labels.append(date)
        else:
            xaxis_labels.append(' ')

    data = [
        dict(
            type="scatter",
            mode="lines+markers",
            name="Retweets",
            x=index,
            y=rts,
            line=dict(shape="spline", width=2, color=color_scale[0]),
            marker=dict(symbol="diamond-open")
        ),
        dict(
            type="scatter",
            mode="lines+markers",
            name="Originales",
            x=index,
            y=ogs,
            line=dict(shape="spline", width=2, color=color_scale[2]),
            marker=dict(symbol="diamond-open")
        ),
        dict(
            type="scatter",
            mode="lines+markers",
            name="Respuestas",
            x=index,
            y=rps,
            line=dict(shape="spline", width=2, color=color_scale[4]),
            marker=dict(symbol="diamond-open")
        ),
        dict(
            type="scatter",
            mode="lines+markers",
            name="Citas",
            x=index,
            y=qts,
            line=dict(shape="spline", width=2, color=color_scale[6]),
            marker=dict(symbol="diamond-open")
        )
    ]

    fig = go.Figure()
    fig.add_trace(go.Scatter(**data[0]))
    fig.add_trace(go.Scatter(**data[1]))
    fig.add_trace(go.Scatter(**data[2]))
    fig.add_trace(go.Scatter(**data[3]))
    fig.update_xaxes(
        ticktext=xaxis_labels,
        tickvals=index
    )
    current_layout['legend'] = dict(x=-.1, y=1.2, font=dict(size=10), orientation='h')
    current_layout['hovermode'] = 'x unified'
    current_layout['title'] = dict(text='Evolución de tweets por fecha', 
                                   xanchor='center', yanchor='top', x=0.5, 
                                   y=0.9, font=dict(color="#777777"))    
    fig.update_layout(current_layout)

    return fig


def create_dist_lang_figure():
    layout_pie = copy.deepcopy(layout)

    languages_no_to_aggregate = ['es', 'ca', 'eu', 'gl', 'en', 'pt', 'fr', 'it']
    df['lang_org'] = df['lang']
    df.loc[~df.lang.isin(languages_no_to_aggregate),'lang'] = 'otro'
    tweets_by_group = df.groupby('lang', as_index=False)['id'].count().sort_values('id', ascending=False)
    values = [
        tweets_by_group.loc[tweets_by_group['lang']=='es','id'].values[0],
        tweets_by_group.loc[tweets_by_group['lang']=='ca','id'].values[0],
        tweets_by_group.loc[tweets_by_group['lang']=='en','id'].values[0],
        tweets_by_group.loc[tweets_by_group['lang']=='otro','id'].values[0],
        tweets_by_group.loc[tweets_by_group['lang']=='pt','id'].values[0],
        tweets_by_group.loc[tweets_by_group['lang']=='eu','id'].values[0],
        tweets_by_group.loc[tweets_by_group['lang']=='gl','id'].values[0],
    ]

    color_scale = px.colors.sequential.Blues_r
    colors = []
    for i in range(len(values)):
        colors.append(color_scale[i])

    data = [
        dict(
            type="pie",
            labels=["Español", "Catalán", "Inglés", "Otro", "Portuges", 
                    "Euskera", "Gallego"],
            values=values,
            name="Distribución de tweets por idioma",
            text=[
                "Total de tweets en Español",
                "Total de tweets en Catalán",
                "Total de tweets en Inglés",
                "Total de tweets en otros idiomas",
                "Total de tweets en Portugues",
                "Total de tweets en Euskera",
                "Total de tweets en Gallego",
            ],
            hoverinfo="text+value+percent",
            textinfo="label+percent+name",
            hole=0.5,
            marker=dict(colors=colors)
        )
    ]
    layout_pie["title"] = 'Distribución de tweets por idioma'
    layout_pie["font"] = dict(color="#777777")
    layout_pie["legend"] = dict(
        font=dict(color="#CCCCCC", size="10"), orientation="h", bgcolor="rgba(0,0,0,0)"
    )

    figure = dict(data=data, layout=layout_pie)
    return figure


def create_location_map():
    with urlopen('https://raw.githubusercontent.com/deldersveld/topojson/master/countries/spain/spain-comunidad.json') as response:
        ccaas = json.load(response)

    fig = px.choropleth(df, geojson=ccaas['objects'], 
                        range_color=(0, 12)
                        )
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

    return fig


def create_dist_users():
    layout_dist_users = copy.deepcopy(layout)

    users_by_date = df.groupby(['date'])['user_screen_name'].nunique().reset_index().sort_values('date', ascending=True)
    users_by_date.rename(columns={'user_screen_name': 'unique_users'}, inplace=True)

    color_scale = px.colors.sequential.Blues_r

    data = [        
        dict(
            type="bar",
            x=users_by_date['date'],
            y=users_by_date['unique_users'],
            name="Usuario Únicos",
            marker=dict(color=color_scale[0]),
            hoverinfo="value",
            textinfo="label+value"
        )
    ]

    layout_dist_users["title"] = "Distribución de usuarios únicos por fecha"

    figure = dict(data=data, layout=layout_dist_users)
    return figure


def create_dist_sentiments():
    layout_dist_users = copy.deepcopy(layout)
    sentiment_colors = get_sentiment_colors()
    sa_df = prepare_sentiment_df()
    dist_sentiments = sa_df.groupby('sentiment_label', as_index=False).size().to_frame(name='count').reset_index()
    dist_sentiments['prop'] = dist_sentiments['count']/sa_df.shape[0]

    labels = [label.title() for label in dist_sentiments['sentiment_label'].values]

    txts = []
    colors = []
    for i in range(len(labels)):
        txts.append("Total de tweets en con tonalidad {}".format(labels[i]))
        colors.append(
            sentiment_colors[labels[i].lower()]
        )

    data = [
        dict(
            type="pie",
            labels=labels,
            values=dist_sentiments['count'].values,
            name="",
            text=txts,
            hoverinfo="text+value+percent",
            textinfo="label+percent",
            hole=0.5,
            marker=dict(colors=colors)
        )
    ]

    layout_dist_users["title"] = 'Distribución de tonalidad de sentimientos en tweets'
    layout_dist_users["font"] = dict(color="#777777")
    layout_dist_users["legend"] = dict(
        font=dict(size="10"), orientation="h", bgcolor="rgba(0,0,0,0)"
    )

    figure = dict(data=data, layout=layout_dist_users)
    return figure


def create_dist_locations():
    layout_dist_locations = copy.deepcopy(layout)

    tweets_by_group = df.groupby('comunidad_autonoma', as_index=False)['id'].count().\
                      sort_values('id', ascending=True)
    tweets_by_group = tweets_by_group[tweets_by_group['comunidad_autonoma']!='desconocido']

    ccaas = tweets_by_group['comunidad_autonoma'].values.tolist()
    num_tweets = tweets_by_group['id'].values.tolist()

    color_scale = px.colors.sequential.Blues_r

    fig = go.Figure(
        go.Bar(
            x=num_tweets,
            y=ccaas,
            orientation='h',
            marker=dict(color=color_scale[0])
        )
    )
    layout_dist_locations['title'] = dict(text='Distribución de tweets por comunidad autónoma', 
                                          xanchor='center', yanchor='top', x=0.5, 
                                          font=dict(color="#777777"))    
    fig.update_layout(layout_dist_locations)

    return fig

def create_dist_unique_users_weekday_figure():
    current_layout = copy.deepcopy(layout)
    color_scale = px.colors.sequential.Blues_r

    if 'day_week' not in list(df.columns):
        df['created_at'] = pd.to_datetime(df['created_at'])
        df['day_week'] = df['created_at'].dt.strftime('%A')
        df.loc[df['day_week']=='Sunday','day_week'] = 'Domingo'
        df.loc[df['day_week']=='Monday','day_week'] = 'Lunes'
        df.loc[df['day_week']=='Tuesday','day_week'] = 'Martes'
        df.loc[df['day_week']=='Wednesday','day_week'] = 'Miércoles'
        df.loc[df['day_week']=='Thursday','day_week'] = 'Jueves'
        df.loc[df['day_week']=='Friday','day_week'] = 'Viernes'
        df.loc[df['day_week']=='Saturday','day_week'] = 'Sábado'
    
    if 'hour' not in list(df.columns):
        df['hour'] = df['created_at'].dt.strftime('%H')

    users_by_day_hour = df.groupby(['day_week','hour'])['user_screen_name'].nunique().reset_index().sort_values('day_week', ascending=True)
    users_by_day_hour.rename(columns={'user_screen_name': 'unique_users'}, inplace=True)

    x_axis = [datetime.time(i).strftime('%H') for i in range(24)]
    y_axis = ['Jueves', 'Viernes', 'Sábado', 'Domingo', 'Lunes', 'Martes', 'Miércoles']
    z = np.zeros((7, 24))
    for ind_y, day in enumerate(y_axis):
        filtered_day = users_by_day_hour.loc[users_by_day_hour['day_week']==day]
        for ind_x, hour in enumerate(x_axis):
            vals = filtered_day.loc[filtered_day['hour']==hour,'unique_users'].values
            if len(vals) > 0:
                num_tweets = int(vals[0])
            else:
                num_tweets = 0
            z[ind_y][ind_x] = num_tweets            

    data = [
        dict(
            x=x_axis,
            y=y_axis,
            z=z,
            xgap=1,
            ygap=1,
            type="heatmap",
            hovertemplate="%{z} tweets publicados el %{y} a las %{x}",
            name="",
            showscale=True,
            colorscale=[[0, color_scale[-1]], [1, color_scale[0]]],
        )
    ]

    current_layout['title'] = "Distribución de usuarios únicos por día y hora"
    current_layout['margin'] = dict(l=70, b=50, t=50, r=50)
    current_layout['modebar'] = dict(orientation='v')
    current_layout['xaxis'] = dict(ticks="", ticklen=2, tickcolor="#ffffff", dtick=1)
    current_layout['yaxis'] = dict(side="left", ticks="", ticksuffix=" ", autorange='reversed')
    current_layout['showlegend'] = True    

    figure = dict(data=data, layout=current_layout)
    return figure


def create_sentiment_evolution_figure():
    current_layout = copy.deepcopy(layout)
    sentiment_colors = get_sentiment_colors()

    sa_df = prepare_sentiment_df()
    tweets_by_group = sa_df.groupby(['date', 'sentiment_label'], as_index=False)['id'].count().sort_values('date', ascending=True)

    dates = tweets_by_group['date'].unique()
    positives = tweets_by_group.loc[tweets_by_group['sentiment_label']=='positivo','id'].values
    negatives = tweets_by_group.loc[tweets_by_group['sentiment_label']=='negativo','id'].values
    neutrals = tweets_by_group.loc[tweets_by_group['sentiment_label']=='neutral','id'].values

    data = [
        dict(
            type='bar',
            name='Negativo',
            x=dates,
            y=negatives,
            marker=dict(color=sentiment_colors['negativo'])
        ),
        dict(
            type='bar',
            name='Neutral', 
            x=dates,
            y=neutrals,
            marker=dict(color=sentiment_colors['neutral'])
        ),
        dict(
            type='bar',
            name='Positivo', 
            x=dates,
            y=positives,
            marker=dict(color=sentiment_colors['positivo'])
        )
    ]

    current_layout['title'] = "Evolución de tonalidad de sentimientos por fecha"
    current_layout["font"] = dict(color="#777777")

    figure = dict(data=data, layout=current_layout)
    return figure


# App layout
app.layout = html.Div(
    [
        dcc.Store(id="aggregate_data"),
        # empty Div to trigger javascript file for graph resizing
        html.Div(id="output-clientside"),
        html.Div(
            [
                html.Div(
                    [
                        html.Img(
                            src=app.get_asset_url("bsc-logo.png"),
                            id="bsc-logo",
                            style={
                                "height": "70px",
                                "width": "auto",
                                "margin-bottom": "25px",
                            },
                        )
                    ],
                    className="one-third column",
                )
            ],
            id="header",
            className="row flex-display",
            style={"margin-bottom": "25px"},
        ),
        html.Div(
            [
                html.Div(
                    children=[
                        generate_menu_bar(), 
                        html.Br(),
                        generate_controls()
                    ],
                    className="pretty_container four columns",
                    id="cross-filter-options",
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Div(                                    
                                    [
                                        html.H2(id="tweets"),
                                        html.P("Total de tweets"),
                                    ],
                                    id="summary_num_tweets",
                                    className="mini_container_tweets",
                                ),
                                html.Div(
                                    [
                                        html.H2(id="rts"),
                                        html.P("Retweets")
                                    ],
                                    id="summary_num_rts",
                                    className="mini_container_rts",
                                ),
                                html.Div(
                                    [
                                        html.H2(id="ogs"),
                                        html.P("Originales")
                                    ],
                                    id="summary_num_ogs",
                                    className="mini_container_ogs",
                                ),
                                html.Div(
                                    [
                                        html.H2(id="rps"),
                                        html.P("Respuestas")
                                    ],
                                    id="summary_num_rps",
                                    className="mini_container_rps",
                                ),
                                html.Div(
                                    [   
                                        html.H2(id="qts"),
                                        html.P("Citas")                                
                                    ],
                                    id="summary_num_qts",
                                    className="mini_container_qts",
                                ),
                                html.Div(
                                    [
                                        html.H2(id="users"),
                                        html.P("Usuarios únicos")                                    
                                    ],
                                    id="summary_num_users",
                                    className="mini_container",
                                ),                                
                            ],
                            id="info-container",
                            className="row flex-display",
                        ),
                        html.Div(
                            [dcc.Graph(
                                id='evo_tweets',
                                figure=create_tweets_evolution_figure()
                                )
                            ],
                            id="evo_tweets_container",
                            className="pretty_container",
                        ),
                    ],
                    id="right-column",
                    className="eight columns",
                ),
            ],
            className="row flex-display",
        ),
        html.Div(
            [
                html.Div(
                    [dcc.Graph(id="tweets_dist", figure=create_dist_locations())],
                    className="pretty_container seven columns",
                ),
                html.Div(
                    [dcc.Graph(id="lang_dist", figure=create_dist_lang_figure())],
                    className="pretty_container five columns",
                ),
            ],
            className="row flex-display",
        ),
        html.Div(
            [
                html.Div(
                    [dcc.Graph(id="unique_users_day_hour_dist", figure=create_dist_unique_users_weekday_figure())],
                    className="pretty_container seven columns",
                ),
                html.Div(
                    [dcc.Graph(id="unique_users_date_dist", figure=create_dist_users())],
                    className="pretty_container five columns",
                ),
            ],
            className="row flex-display",
        ),
        html.Div(
            [
                html.Div(
                    [dcc.Graph(id="evo_sentiments", figure=create_sentiment_evolution_figure())],
                    className="pretty_container seven columns",
                ),
                html.Div(
                    [dcc.Graph(id="sentiments_dist", figure=create_dist_sentiments())],
                    className="pretty_container five columns",
                ),
            ],
            className="row flex-display",
        ),
    ],
    id="mainContainer",
    style={"display": "flex", "flex-direction": "column"},
)


if __name__ == '__main__':
    app.run_server(debug=True)