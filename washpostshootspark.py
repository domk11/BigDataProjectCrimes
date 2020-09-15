import pandas as pd
import squarify

import numpy as np
import seaborn as sns
from src.spark import create_session, create_df, COLUMNS
from pyspark.sql.functions import *
from src.database.contracts import wash_contract as c
import matplotlib.pyplot as plt
import os
import datetime
import re
from collections import Counter

from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
from pyspark.sql.types import DateType
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
from pyspark.sql.types import *
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from pyspark.sql.functions import udf

def _map_to_pandas(rdds):
    """ Needs to be here due to pickling issues """
    return [pd.DataFrame(list(rdds))]

def toPandas(df, n_partitions=None):
    """
    Returns the contents of `df` as a local `pandas.DataFrame` in a speedy fashion. The DataFrame is
    repartitioned if `n_partitions` is passed.
    :param df:              pyspark.sql.DataFrame
    :param n_partitions:    int or None
    :return:                pandas.DataFrame
    """
    if n_partitions is not None: df = df.repartition(n_partitions)
    df_pand = df.rdd.mapPartitions(_map_to_pandas).collect()
    df_pand = pd.concat(df_pand)
    df_pand.columns = df.columns
    return df_pand

def get_year(year_str):
    # expects format: 2009-01-01
    return int(str(year_str)[:4])
udf_get_year = udf(get_year, IntegerType())

def get_month(month_str):
    # expects format: 2009-01-01
    return int(str(month_str).split('-')[1])
udf_get_month = udf(get_month, IntegerType())

def convert_race(race):
    return {
        'W':'White',
        'B':'Black',
        'N':'Native American',
        'H':'Hispanic',
        'A':'Asian',
        'O':'Others'
    }.get(race)

def mkfeature(data):
    df = data.select(c.ID, c.DATE,c.RACE,c.AGE,c.STATE, c.ARMED, c.FLEE)
    #shoot = toPandas(df)
    func =  udf(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'), DateType())
    convert = udf(convert_race)
    shoot = df.withColumn('date', func(col('date')))
    shoot =shoot.withColumn('year', udf_get_year('date'))
    shoot =shoot.withColumn('month', udf_get_month('date'))
    shoot= shoot.withColumn('month_num', udf_get_month('date'))
    #shoot['race_name']=np.where(shoot['race']=='W','White',np.where(shoot['race']=='B','Black', np.where(shoot['race']=='N','Native American',np.where(shoot['race']=='H','Hispanic', np.where(shoot['race']=='A','Asian',np.where(shoot['race']=='O','Others','Not Specified'))))))
    shoot=shoot.withColumn('race_name', convert(c.RACE))
    return shoot

def convert_date(date):

    year = date.strftime('%Y')
    month= date.strftime('%m')
    return f'{year}-{month}'

def convert_armed(armed):
    if armed != 'unarmed':
        return 'armed'
    else:
        return 'unarmed'

def monthly(data):

    convert = udf(convert_date)

    monthly_df = data.select(c.DATE).withColumn(c.DATE,convert(c.DATE)).groupby(c.DATE).count().sort(col(c.DATE))
    #monthly_df.show()
    monthly_df = toPandas(monthly_df)

    month_year=[]
    for i in monthly_df['date']:
        month_year.append(str(i))
    fig = make_subplots(
        rows=2, cols=1,
        subplot_titles=("Monthly series", "Distribution of monthly count"))

    fig.add_trace(go.Scatter(
        x= month_year,
        y= monthly_df['count'],
        name="Monthly Deaths",
        mode='lines',
    ),row=1,col=1)

    fig.add_trace(go.Box(y=monthly_df['count'], name='Count',
                         marker_color = 'indianred',boxmean='sd'),row=2,col=1
                  )
    fig.update_xaxes(title_text="Year", row=1, col=1,showline=True, linewidth=2, linecolor='black', mirror=True)
    fig.update_xaxes(title_text=" ", row=2, col=1,showline=True, linewidth=2, linecolor='black', mirror=True)
    fig.update_yaxes(title_text="Number of Victims", row=1, col=1,showline=True, linewidth=2, linecolor='black', mirror=True)
    fig.update_yaxes(title_text="Number of Victims", row=2, col=1,showline=True, linewidth=2, linecolor='black', mirror=True)

    fig.update_layout(title_text='Fatal Killing Monthly Count 2015 - 2020', title_x=0.5,showlegend=False,height=1000)
    fig.write_image('monthlysp.png')
    return monthly_df

def plot_month(monthly_df, year,color):
    temp_month=[]
    for i in monthly_df.loc[monthly_df['year']==year]['date']:
        temp_month.append(str(i))
    trace=go.Bar(x=temp_month, y=monthly_df.loc[monthly_df['year']==year]['count'],
             name=year,marker_color=color)
    return trace

def yearly(monthly_df):
    monthly_df['year']=monthly_df['date'].dt.strftime('%Y')
    fig = make_subplots(rows=3, cols=2,subplot_titles=("2015", "2016","2017","2018","2019","2020"))
    fig.add_trace(plot_month(monthly_df, '2015','red'),row=1,col=1)
    fig.add_trace(plot_month(monthly_df, '2016','blue'),row=1,col=2)
    fig.add_trace(plot_month(monthly_df, '2017','green'),row=2,col=1)
    fig.add_trace(plot_month(monthly_df, '2018','orange'),row=2,col=2)
    fig.add_trace(plot_month(monthly_df, '2019','indigo'),row=3,col=1)
    fig.add_trace(plot_month(monthly_df, '2020','violet'),row=3,col=2)
    fig.update_xaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
    fig.update_yaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
    fig.update_layout(title_text='Deaths Every Year', title_x=0.5,showlegend=False)
    #fig.write_image('yearly.png')

def kills_per_year(data):

    year_shoot = data.select('year').groupby('year').count().sort(col('year'))

    year_shoot=toPandas(year_shoot)

    fig = go.Figure(data=go.Scatter(
        x= year_shoot['year'],
        y= year_shoot['count'],
        mode='lines+markers',
        marker_color="red"
    ))
    fig.update_xaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
    fig.update_yaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
    fig.update_layout(title_text='Deaths - All Years',xaxis_title='Years',
                  yaxis_title='Total number of kills', title_x=0.5)
    fig.write_image('killsyearsp.png')

def agehist(shoot):
    hist_data = [shoot['age'].dropna()]
    group_labels = ['Age']
    colors=["blue"]
    fig=go.Figure()
    fig=ff.create_distplot(hist_data, group_labels,bin_size=5,colors=colors)
    fig.update_layout(title_text="Distribution of Age",title_x=0.5)
    fig.write_image('agehist.png')

def plot_month_race(shoot_race, race,color):
    temp_month=[]
    for i in shoot_race.loc[(shoot_race['race_name']==race)]['monthly']:
        temp_month.append(str(i))
    trace=go.Bar(x=temp_month, y= shoot_race.loc[(shoot_race['race_name']==race)]['count'],
                 name=race,marker_color=color)
    return trace
def races(shoot):
    #shoot_race=shoot.groupby(['year','month_num','race_name']).agg('count')['id'].to_frame(name='count').reset_index()

    shoot_race=shoot.groupby(['year','month_num','race_name']).count()
    shoot_race.head()
    shoot_race=toPandas(shoot_race)
    shoot_race['monthly']=shoot_race['year'].astype(str)+"-"+shoot_race['month_num'].astype(str)

    fig = make_subplots(rows=3, cols=2,subplot_titles=("Black", "White","Hispanic","Asian","Native American","Others"))
    fig.add_trace(plot_month_race(shoot_race, 'Black','brown'),row=1,col=1)
    fig.add_trace(plot_month_race(shoot_race, 'White','deepskyblue'),row=1,col=2)
    fig.add_trace(plot_month_race(shoot_race, 'Hispanic','green'),row=2,col=1)
    fig.add_trace(plot_month_race(shoot_race, 'Asian','red'),row=2,col=2)
    fig.add_trace(plot_month_race(shoot_race, 'Native American','orange'),row=3,col=1)
    fig.add_trace(plot_month_race(shoot_race, 'Others','violet'),row=3,col=2)
    fig.update_layout(title_text='Deaths - All Race',title_x=0.5)
    #fig.write_image('affrace.png')

    fig = go.Figure()
    fig.add_trace(go.Box(y=shoot_race.loc[(shoot_race['race_name']=="Black")]['count'], name='Black',
                         marker_color = 'brown',boxmean=True))
    fig.add_trace(go.Box(y=shoot_race.loc[(shoot_race['race_name']=="White")]['count'], name = 'White',
                         marker_color = 'grey',boxmean=True ))
    fig.add_trace(go.Box(y=shoot_race.loc[(shoot_race['race_name']=="Hispanic")]['count'], name = 'Hispanic',
                         marker_color = 'green',boxmean=True ))
    fig.add_trace(go.Box(y=shoot_race.loc[(shoot_race['race_name']=="Asian")]['count'], name = 'Asian',
                         marker_color = 'red',boxmean=True ))
    fig.add_trace(go.Box(y=shoot_race.loc[(shoot_race['race_name']=="Native American")]['count'], name = 'Native American',
                         marker_color = 'orange',boxmean=True ))
    fig.add_trace(go.Box(y=shoot_race.loc[(shoot_race['race_name']=="Others")]['count'], name = 'Others',
                         marker_color = 'violet',boxmean=True ))


    fig.update_xaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
    fig.update_yaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
    fig.update_layout(title="Death Toll - All Races",title_x=0.5,xaxis=dict(title="Race"),yaxis=dict(title="Number of Victims"))
    fig.write_image('racetoll.png')

def crimesperstate(shoot):

    shoot_state=shoot.select('state').groupby('state').count().sort(col('count'))

    shoot_state=toPandas(shoot_state)

    fig = go.Figure(go.Bar(
        y= shoot_state['state'],
        x= shoot_state['count'],
        orientation='h',
        text=shoot_state['count'],
        textposition='outside',
        marker_color=shoot_state['count'],
    ))
    fig.update_xaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
    fig.update_yaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
    fig.update_layout(title_text='Fatal Killing - All States',yaxis_title='States',
                  xaxis_title='Total number of victims', title_x=0.5,height=1000)
    fig.write_image('crimesperstatesp.png')

def armed(shoot):
    armed=list(shoot['armed'].dropna().unique())
    fig, (ax2) = plt.subplots(1,1,figsize=[17, 10])
    wordcloud2 = WordCloud(background_color='black',colormap="OrRd_r",
                           width=600,height=400).generate(" ".join(armed))
    ax2.imshow(wordcloud2,interpolation='bilinear')
    ax2.axis('off')
    ax2.set_title('Most Used Arms',fontsize=35)
    plt.savefig('armed.png')

def flee(shoot):
    arm_df=shoot['flee'].value_counts().reset_index().rename(columns={'index':'flee','flee':'count'})
    fig = go.Figure(data=[go.Pie(labels=arm_df['flee'], values=arm_df['count'], hole=.3,textinfo='label+percent',
                                 insidetextorientation='radial')])
    fig.update_layout(title_text='Victim Flee?', title_x=0.5)
    fig.write_image('flee.png')

def armedornot(shoot):
    convert = udf(convert_armed)
    shoot = shoot.filter(col('state')=='NY')
    arm_df=shoot.select('armed', 'race_name').na.drop(subset=["armed"]).na.drop(subset=["race_name"])
    arm_df=arm_df.withColumn('armed', convert('armed')).groupby('race_name','armed').count()
    arm_df1 = arm_df.filter(col('armed') == 'armed')
    arm_dfpd=toPandas(arm_df1)
    fig = go.Figure(data=[go.Pie(labels=arm_dfpd['race_name'], values=arm_dfpd['count'], hole=.3,textinfo='label+percent',
                            insidetextorientation='radial')])
    fig.update_layout(title_text='armed or not', title_x=0.5)
    fig.write_image('armedny.png')
    arm_df2 = arm_df.filter(col('armed') == 'unarmed')
    arm_dfpd=toPandas(arm_df2)
    fig = go.Figure(data=[go.Pie(labels=arm_dfpd['race_name'], values=arm_dfpd['count'], hole=.3,textinfo='label+percent',
                                 insidetextorientation='radial')])
    fig.update_layout(title_text='armed or not', title_x=0.5)
    fig.write_image('armednotny.png')

def blacklivesmatter(shoot):
    #black_state=shoot[shoot['race']=='B']['state'].value_counts().to_frame().reset_index().rename(columns={'index':'state','state':'count'})
    #black_year=shoot[shoot['race']=='B']['year'].value_counts().to_frame().reset_index().rename(columns={'index':'year','year':'count'})
    black=shoot.filter(col('race')=='B')
    black_state=black.select('race','state').groupby('state').count().toPandas()
    black_year=black.select('race','year').groupby('year').count().toPandas()
    shoota=shoot.select('race','age').toPandas()
    fig = make_subplots(
        rows=2, cols=2, subplot_titles=("Black People victims in each states", "Yearly Black Victims", "Distribution of Black people Age",),
        column_widths=[0.6, 0.4],
        row_heights=[0.4, 0.6],
        specs=[[{"type": "Choropleth", "rowspan": 2}, {"type": "bar"}],
               [            None                    , {"type": "histogram"}]])

    fig.add_trace(go.Choropleth(
        locations=black_state['state'],
        z=black_state['count'].astype(float),
        locationmode='USA-states',
        colorscale='Reds',
        autocolorscale=False,
        text=black_state['state'],
        marker_line_color='white',
        colorbar_title="Millions USD",showscale = False,
    ),row=1, col=1)

    fig.add_trace(go.Bar(
        x= black_year['year'],
        y= black_year['count'],
        text=black_year['count'],
        textposition='inside',
        marker_color="chocolate",
        name="Death by year"
    ),row=1, col=2)

    fig.add_trace(go.Histogram(x=shoota[shoota['race']=='B']["age"],nbinsx=10,marker_color='brown',name="Age Distribution",xbins=dict(size=10),
                              opacity=1),row=2, col=2)

    fig.update_layout(
        title_text='US Police Killing Black Peoples (2015-2020)',
        title_x=0.5,
        geo = dict(
            scope='usa',
            projection=go.layout.geo.Projection(type = 'albers usa'),
            showlakes=True, # lakes
            lakecolor='rgb(255, 255, 255)'),
    )

    fig.update_layout(
        template="plotly_dark",showlegend=False)
    fig.write_image('blacklivesmattersp.png')

def allrace(shoot):
    #year_shoot=shoot['year'].value_counts().to_frame().reset_index().rename(columns={'index':'year','year':'count'}).sort_values(by="year")
   # shoot_state=shoot['state'].value_counts().to_frame().reset_index().rename(columns={'index':'state','state':'count'}).sort_values(by='count',ascending=False)
   # only_race=shoot[shoot['race_name']!="Not Specified"]['race_name'].value_counts().to_frame().reset_index().rename(columns={'index':'race_name','race_name':'count'})
    year_shoot=shoot.select('year').groupby('year').count().toPandas()
    shoot_state=shoot.select('state').groupby('state').count().toPandas()
    shootb=shoot.filter(col('race_name')!='Others')
    only_race=shootb.select('race_name').groupby('race_name').count().toPandas()

    fig = make_subplots(
        rows=2, cols=2, subplot_titles=("Victims in all states", "Victims by Year", "Victims by Race",),
        column_widths=[0.6, 0.4],
        row_heights=[0.4, 0.6],
        specs=[[{"type": "Choropleth", "rowspan": 2}, {"type": "bar"}],
               [            None                    , {"type": "pie"}]])

    fig.add_trace(go.Choropleth(
        locations=shoot_state['state'],
        z=shoot_state['count'].astype(float),
        locationmode='USA-states',
        colorscale='Reds',
        autocolorscale=False,
        text=shoot_state['state'],
        marker_line_color='white',
        colorbar_title="Millions USD",showscale = False,
    ),row=1, col=1)

    fig.add_trace(go.Bar(
        x= year_shoot['year'],
        y= year_shoot['count'],
        text=year_shoot['count'],
        textposition='inside',
        marker_color="chocolate",
        name="Death by year"
    ),row=1, col=2)
    fig.add_trace(go.Pie(labels=only_race['race_name'], values=only_race['count'],textinfo='percent',
                         insidetextorientation='radial'),row=2, col=2)


    fig.update_layout(
        title_text='US Police Killing All Race (2015-2020)',
        title_x=0.5,
        geo = dict(
            scope='usa',
            projection=go.layout.geo.Projection(type = 'albers usa'),
            showlakes=True, # lakes
            lakecolor='rgb(255, 255, 255)'),
    )
    fig.update_layout(
        template="plotly_dark")
    fig.write_image('allstateracessp.png')

def main():
    spark = create_session(c.COLLECTION_NAME)
    spark.sparkContext.setLogLevel('ERROR')

    try:

        wpsdf = create_df(spark).cache()
        wpf = wpsdf.select(c.ID, c.DATE,c.RACE,c.AGE,c.ARMED,c.BODYCAM,c.DEATH_MANNER,c.CITY,c.THREAT_LEVEL,c.SEX,c.STATE, c.FLEE)
        shoot = mkfeature(wpf)
        #monthly_df = monthly(shoot)
        #yearly(monthly_df)
        #kills_per_year(shoot)
        #agehist(shoot)
        #races(shoot)
        #crimesperstate(shoot)
        #armed(shoot)
        #armedornot(shoot)
        #flee(shoot)
        #blacklivesmatter(shoot)
        allrace(shoot)

    except Exception as e:
        print(e)
        spark.stop()


if __name__ == '__main__':
    main()
