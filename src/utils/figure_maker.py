
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import seaborn as sns



def lineplot(df, x_axis_name, y_axis_name, x_axis_label, y_axis_label,
             font_size_x_axis_label, font_size_y_axis_label, x_ticks_size, 
             y_ticks_size, x_ticks_rotation, aesthetic_options, figure_size=(14,7)):
    plt.figure(figsize=figure_size)
    params = {
        'x': df[x_axis_name],
        'y': df[y_axis_name],        
    }
    params.update(aesthetic_options)
    fig = sns.lineplot(**params)
    fig.set_xlabel(x_axis_label, fontsize=font_size_x_axis_label)
    fig.set_ylabel('Score Sentimiento',fontsize=font_size_y_axis_label)
    plt.yticks(size=y_ticks_size)
    #plt.ylim((-1,1))
    fig.xaxis.set_major_locator(mdates.DayLocator())
    fig.xaxis.set_major_formatter(mdates.DateFormatter('%d-%m-%Y'))
    plt.setp(fig.xaxis.get_majorticklabels(), rotation=x_ticks_rotation, 
             size=x_ticks_size)
    return fig


def bars_by_date(df, bars_data, x_axis_label, y_axis_label, 
                font_size_x_axis_label, font_size_y_axis_label, 
                x_ticks_size, y_ticks_size, bar_width, x_ticks_rotation,
                label_bars=True, figure_size=(14,7)):

    fig, ax = plt.subplots(figsize=(15,7))
    
    # dates
    xaxis_values = [date.strftime('%d-%m-%Y') for date in df.index.unique()]

    # Define position of bars
    bars_data[0]['position'] = np.arange(len(bars_data[0]['values']))
    for i in range(len(bars_data)):
        if i==0:
            continue
        bars_data[i]['position'] = [x + bar_width for x in bars_data[i-1]['position']]    

    bars = []
    for bar_data in bars_data:
        rects = plt.bar(bar_data['position'], bar_data['values'], width=bar_width, 
                        color=bar_data['color'], edgecolor=bar_data['edgecolor'], 
                        capsize=bar_data['capsize'], label=bar_data['label'])
        if label_bars:
            for rect in rects:
                height = rect.get_height()
                ax.annotate('{0:,}'.format(height),
                            xy=(rect.get_x() + rect.get_width() / 2, height),
                            xytext=(0, 3),  # 3 points vertical offset
                            textcoords="offset points",
                            ha='center', va='bottom', fontsize=13, color='black')
        bars.append(rects)

    # general layout
    plt.xticks([r + bar_width for r in range(len(bars_data[0]['values']))], 
        xaxis_values, size=x_ticks_size, rotation=x_ticks_rotation)
    plt.xlabel(x_axis_label, fontsize=font_size_x_axis_label)
    plt.ylabel(y_axis_label, fontsize=font_size_y_axis_label)    
    plt.yticks(size=y_ticks_size)
    plt.legend()
    
    return fig


def donut(data, aesthetic_options, axis_equal=True):
    group_names = []
    values = []
    colors = []
    for datum in data:
        if 'percentages' in datum:
            group_names.append(
                '{0}: {1} ({2}%)'.format(datum['label'], str(datum['values']), 
                                        str(datum['percentages']))
            )
        else:
            group_names.append(
                '{0}: {1}%'.format(datum['label'], str(datum['values']))
            )
        values.append(datum['values'])
        colors.append(datum['color'])

    radius = aesthetic_options['radius']
    del aesthetic_options['radius']
    font_size = aesthetic_options['font_size']
    del aesthetic_options['font_size']
    width = aesthetic_options['width']
    del aesthetic_options['width']

    group_size=values
    fig, ax = plt.subplots()
    if axis_equal:
        ax.axis('equal')
    
    pie, _ = ax.pie(group_size, radius=radius, labels=group_names, 
                    wedgeprops = aesthetic_options, colors=colors,
                    textprops={'fontsize': font_size})
    plt.setp(pie, width=width, edgecolor=aesthetic_options['edgecolor'])
    return fig


def hlines(df, x_axis_name, y_axis_name, x_axis_label, y_axis_label, 
           font_size_x_axis_label, font_size_y_axis_label, x_ticks_size, 
           y_ticks_size, line_color, marker_color, figure_size=(12,9)):
    plt.figure(figsize=figure_size)
    fig_range=range(1,len(df.index)+1)
    fig=plt.hlines(y=fig_range, xmin=0, xmax=df[x_axis_name], 
                   color=line_color)
    plt.plot(df[x_axis_name], fig_range, "o", color=marker_color)
    plt.yticks(fig_range, df[y_axis_name])
    plt.xlabel(x_axis_label, size=font_size_x_axis_label)
    plt.ylabel(y_axis_label, size=font_size_y_axis_label)
    plt.xticks(size=x_ticks_size)
    plt.yticks(size=y_ticks_size)
    return fig