import pandas as pd
import squarify

import numpy as np
import seaborn as sns
from src.spark import create_session, create_df, COLUMNS
import pyspark.sql.functions as F
from src.database.contracts import wash_contract as c
import matplotlib.pyplot as plt
import os
import datetime
import re
from collections import Counter

from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator

import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize


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

def mkfeature(data):
    df = data.select(c.DATE,c.RACE,c.AGE)
    shoot = toPandas(df)
    shoot['date']=pd.to_datetime(shoot['date'])
    shoot['year']=pd.to_datetime(shoot['date']).dt.year
    shoot['month']=pd.to_datetime(shoot['date']).dt.month
    shoot['month_name']=shoot['date'].dt.strftime('%B')
    shoot['month_num']=shoot['date'].dt.strftime('%m')
    shoot['weekdays']=shoot['date'].dt.strftime('%A')
    shoot['date_num']=shoot['date'].dt.strftime('%d').astype(int)
    shoot['date_categ']=np.where(shoot['date_num']<16,"First Half","Second Half")
    shoot['date_mon']=shoot.date.dt.to_period("M")
    #shoot['age_freq']=np.where(shoot['age']<18,'<18',np.where((shoot['age']>17)&(shoot['age']<=30),'18-30', np.where((shoot['age']>30)&(shoot['age']<=40),'31-40',np.where(shoot['age']>50,'50+', np.where((shoot['age']>40)&(shoot['age']<=50),'41-50',"Not Specified")))))
    #shoot['race_name']=np.where(shoot['race']=='W','White',np.where(shoot['race']=='B','Black', np.where(shoot['race']=='N','Native American',np.where(shoot['race']=='H','Hispanic', np.where(shoot['race']=='A','Asian',np.where(shoot['race']=='O','Others','Not Specified'))))))
    return shoot

def monthly(data):
    monthly_df=data['date'].groupby(data.date.dt.to_period("M")).agg('count').to_frame(name="count").reset_index()
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
    fig.write_image('monthly.png')

def main():
    spark = create_session(c.COLLECTION_NAME)
    spark.sparkContext.setLogLevel('ERROR')

    try:

        wpsdf = create_df(spark).cache()
        wpf = wpsdf.select(c.DATE,c.RACE,c.AGE,c.ARMED,c.BODYCAM,c.DEATH_MANNER,c.CITY,c.THREAT_LEVEL,c.SEX,c.STATE)
        shoot = mkfeature(wpf)
        monthly(shoot)



    except Exception as e:
        print(e)
        spark.stop()


if __name__ == '__main__':
    main()
