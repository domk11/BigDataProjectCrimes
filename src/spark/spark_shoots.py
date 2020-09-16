import matplotlib.pyplot as plt
from pyspark import StorageLevel
from pyspark.sql.functions import *
from pyspark.sql.types import *
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import pandas as pd
from src.database.contracts import wash_contract as c
from utils import self_toPandas, udf_get_month, udf_convert_race_shoots, \
                  udf_convert_date_to_datetime, udf_convert_date_to_string, \
                  udf_convert_armed, udf_get_year


class SparkShoots:

    def __init__(self, shoots_df):
        self.shoots_df = shoots_df
        self._preprocess()

    def _preprocess(self):
        self.shoots_df = self.shoots_df.select(c.ID, c.DATE, c.RACE, c.AGE, c.STATE, c.ARMED, c.FLEE) \
                                       .withColumn(c.DATE, udf_convert_date_to_datetime(col(c.DATE))) \
                                       .withColumn('year', udf_get_year(c.DATE)) \
                                       .withColumn('month', udf_get_month(c.DATE)) \
                                       .withColumn('month_num', udf_get_month(c.DATE)) \
                                       .withColumn('race_name', udf_convert_race_shoots(c.RACE))
        self.shoots_df.persist(StorageLevel.MEMORY_AND_DISK).count()

    def _save_csv(self, df, csv_out):
        df.to_csv(csv_out)

    def monthly(self, img_out=False, csv_out=False):
        monthly_df = self.shoots_df.select(c.DATE)\
                         .withColumn(c.DATE, udf_convert_date_to_string(c.DATE))\
                         .groupby(c.DATE)\
                         .count()\
                         .sort(col(c.DATE))

        monthly_df = self_toPandas(monthly_df)
        month_year = [str(i) for i in monthly_df[c.DATE]]

        if csv_out:
            self._save_csv(monthly_df, 'monthlysp.csv')

        if img_out:
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('Monthly series', 'Distribution of monthly count')
            )
            fig.add_trace(go.Scatter(
                x=month_year,
                y=monthly_df['count'],
                name='Monthly Deaths',
                mode='lines',
            ), row=1, col=1)
            fig.add_trace(go.Box(
                y=monthly_df['count'],
                name='Count',
                marker_color='indianred',
                boxmean='sd'
            ), row=2, col=1)
            fig.update_xaxes(title_text='Year', row=1, col=1, showline=True, linewidth=2, linecolor='black', mirror=True)
            fig.update_xaxes(title_text=' ', row=2, col=1, showline=True, linewidth=2, linecolor='black', mirror=True)
            fig.update_yaxes(title_text='Number of Victims', row=1, col=1, showline=True, linewidth=2, linecolor='black', mirror=True)
            fig.update_yaxes(title_text='Number of Victims', row=2, col=1, showline=True, linewidth=2, linecolor='black', mirror=True)
            fig.update_layout(title_text='Fatal Killing Monthly Count 2015 - 2020', title_x=0.5, showlegend=False, height=1000)
            fig.write_image('monthlysp.png')

        return monthly_df

    def plot_month(self, monthly_df, year, color):
        temp_month = [str(i) for i in monthly_df.loc[monthly_df['year'] == year][c.DATE]]
        return go.Bar(x=temp_month, y=monthly_df.loc[monthly_df['year'] == year]['count'], name=year, marker_color=color)

    def yearly(self, monthly_df, img_out=False, csv_out=False):
        monthly_df[c.DATE] = pd.to_datetime(monthly_df[c.DATE])
        monthly_df['year'] = monthly_df['date'].dt.strftime('%Y')

        if csv_out:
            self._save_csv(monthly_df, 'yearly.csv')

        if img_out:
            fig = make_subplots(rows=3, cols=2, subplot_titles=('2015', '2016', '2017', '2018', '2019', '2020'))
            fig.add_trace(self.plot_month(monthly_df, '2015', 'red'), row=1, col=1)
            fig.add_trace(self.plot_month(monthly_df, '2016', 'blue'), row=1, col=2)
            fig.add_trace(self.plot_month(monthly_df, '2017', 'green'), row=2, col=1)
            fig.add_trace(self.plot_month(monthly_df, '2018', 'orange'), row=2, col=2)
            fig.add_trace(self.plot_month(monthly_df, '2019', 'indigo'), row=3, col=1)
            fig.add_trace(self.plot_month(monthly_df, '2020', 'violet'), row=3, col=2)
            fig.update_xaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
            fig.update_yaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
            fig.update_layout(title_text='Deaths Every Year', title_x=0.5, showlegend=False)
            fig.write_image('yearly.png')

    def kills_per_year(self, img_out=False, csv_out=False):
        year_shoot = self.shoots_df.select('year')\
                         .groupby('year')\
                         .count()\
                         .sort(col('year'))

        year_shoot = self_toPandas(year_shoot)

        if csv_out:
            self._save_csv(year_shoot, 'kills_year_sp.csv')

        if img_out:
            fig = go.Figure(data=go.Scatter(
                x=year_shoot['year'],
                y=year_shoot['count'],
                mode='lines+markers',
                marker_color='red'
            ))
            fig.update_xaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
            fig.update_yaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
            fig.update_layout(title_text='Deaths - All Years', xaxis_title='Years', yaxis_title='Total number of kills', title_x=0.5)
            fig.write_image('kills_year_sp.png')

    def agehist(self):
        hist_data = self.shoots_df.select(c.AGE).filter(length(col(c.AGE))>0)
        hist_data = [hist_data.toPandas()[c.AGE].astype(int).dropna()]
        group_labels = ['Age']
        colors = ['blue']
        fig = go.Figure()
        fig = ff.create_distplot(hist_data, group_labels, bin_size=5, colors=colors)
        fig.update_layout(title_text='Distribution of Age', title_x=0.5)
        fig.write_image('agehist.png')

    def plot_month_race(self, shoot_race, race, color):
        temp_month = [str(i) for i in shoot_race.loc[(shoot_race['race_name'] == race)]['monthly']]
        return go.Bar(x=temp_month, y=shoot_race.loc[(shoot_race['race_name'] == race)]['count'], name=race, marker_color=color)

    def races(self, img_out=False, csv_out=False):
        shoot_race = self.shoots_df.groupby(['year', 'month_num', 'race_name']).count()
        shoot_race = self_toPandas(shoot_race)
        shoot_race['monthly'] = shoot_race['year'].astype(str) + '-' + shoot_race['month_num'].astype(str)

        if csv_out:
            self._save_csv(shoot_race, f'racetoll.csv')

        if img_out:
            fig = make_subplots(rows=3, cols=2, subplot_titles=('Black', 'White', 'Hispanic', 'Asian', 'Native American', 'Others'))
            fig.add_trace(self.plot_month_race(shoot_race, 'Black', 'brown'), row=1, col=1)
            fig.add_trace(self.plot_month_race(shoot_race, 'White', 'deepskyblue'), row=1, col=2)
            fig.add_trace(self.plot_month_race(shoot_race, 'Hispanic', 'green'), row=2, col=1)
            fig.add_trace(self.plot_month_race(shoot_race, 'Asian', 'red'), row=2, col=2)
            fig.add_trace(self.plot_month_race(shoot_race, 'Native American', 'orange'), row=3, col=1)
            fig.add_trace(self.plot_month_race(shoot_race, 'Others', 'violet'), row=3, col=2)
            fig.update_layout(title_text='Deaths - All Race', title_x=0.5)
            fig.write_image('affrace.png')

            fig = go.Figure()
            races = ['Black', 'White', 'Hispanic', 'Asian', 'Native American', 'Others']
            colors = ['brown', 'grey', 'green', 'red', 'orange', 'violet']
            for race, color in zip(races, colors):
                fig.add_trace(go.Box(y=shoot_race.loc[(shoot_race['race_name'] == race)]['count'], name=race, marker_color=color, boxmean=True))
            fig.update_xaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
            fig.update_yaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
            fig.update_layout(title='Death Toll - All Races', title_x=0.5, xaxis=dict(title='Race'), yaxis=dict(title='Number of Victims'))
            fig.write_image('racetoll.png')

    def crimes_per_state(self, img_out=False, csv_out=False):
        shoot_state = self.shoots_df.select(c.STATE)\
                           .groupby(c.STATE)\
                           .count()\
                           .sort(col('count'))

        shoot_state = self_toPandas(shoot_state)

        if csv_out:
            self._save_csv(shoot_state, 'crimes_per_states.csv')

        if img_out:
            fig = go.Figure(go.Bar(
                y=shoot_state[c.STATE],
                x=shoot_state['count'],
                orientation='h',
                text=shoot_state['count'],
                textposition='outside',
                marker_color=shoot_state['count'],
            ))
            fig.update_xaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
            fig.update_yaxes(showline=True, linewidth=2, linecolor='black', mirror=True)
            fig.update_layout(title_text='Fatal Killing - All States', yaxis_title='States',
                              xaxis_title='Total number of victims', title_x=0.5, height=1000)
            fig.write_image('crimes_per_states.png')

    def armed(self):
        armed = self.shoots_df.select('armed').toPandas()
        armed = list(armed['armed'].dropna().unique())
        fig, ax = plt.subplots(1, 1, figsize=[17, 10])
        wc = WordCloud(background_color='black', colormap='OrRd_r', width=600, height=400).generate(' '.join(armed))
        ax.imshow(wc, interpolation='bilinear')
        ax.axis('off')
        ax.set_title('Most Used Arms', fontsize=35)
        plt.savefig('armed.png')

    def flee(self, img_out=False, csv_out=False):
        arm_df = self.shoots_df.select(c.FLEE).groupby(c.FLEE).count()
        arm_df = self_toPandas(arm_df)
        if csv_out:
            self._save_csv(arm_df, 'flee.csv')

        if img_out:
            fig = go.Figure(data=[
                go.Pie(
                    labels=arm_df['flee'],
                    values=arm_df['count'],
                    hole=.3,
                    textinfo='label+percent',
                    insidetextorientation='radial'
                )
            ])
            fig.update_layout(title_text='Victim Flee?', title_x=0.5)
            fig.write_image('flee.png')

    def armed_or_not(self, img_out=False, csv_out=False):
        shoot = self.shoots_df.filter(col(c.STATE) == 'NY')
        arm_df = shoot.select(c.ARMED, 'race_name')\
                      .na.drop(subset=[c.ARMED])\
                      .na.drop(subset=['race_name'])

        arm_df = arm_df.withColumn(c.ARMED, udf_convert_armed(c.ARMED))\
                       .groupby('race_name', c.ARMED)\
                       .count()

        for label in ['armed', 'unarmed']:
            _arm_df = arm_df.filter(col(c.ARMED) == label)
            arm_dfpd = self_toPandas(_arm_df)

            if csv_out:
                self._save_csv(arm_dfpd, f'{label}.csv')

            if img_out:
                fig = go.Figure(data=[
                    go.Pie(
                        labels=arm_dfpd['race_name'],
                        values=arm_dfpd['count'],
                        hole=.3,
                        textinfo='label+percent',
                        insidetextorientation='radial'
                    )
                ])
                fig.update_layout(title_text=f'New York: {label}', title_x=0.5)
                fig.write_image(f'{label}_ny.png')

    def blacklivesmatter(self, img_out=False, csv_out=False):
        black = self.shoots_df.filter(col(c.RACE) == 'B')
        black_state = black.select(c.RACE, c.STATE).groupby(c.STATE).count().toPandas()
        black_year = black.select(c.RACE, 'year').groupby('year').count().toPandas()
        shoota = self.shoots_df.select(c.RACE, c.AGE).toPandas()

        if csv_out:
            self._save_csv(black_state, 'black_state.csv')
            self._save_csv(black_year, 'black_year.csv')
            self._save_csv(shoota, 'shoota.csv')

        if img_out:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Black People victims in each states', 'Yearly Black Victims', 'Distribution of Black people Age',),
                column_widths=[0.6, 0.4],
                row_heights=[0.4, 0.6],
                specs=[
                    [{'type': 'Choropleth', 'rowspan': 2}, {'type': 'bar'}],
                    [None, {'type': 'histogram'}]
                ]
            )
            fig.add_trace(go.Choropleth(
                locations=black_state[c.STATE],
                z=black_state['count'].astype(float),
                locationmode='USA-states',
                colorscale='Reds',
                autocolorscale=False,
                text=black_state[c.STATE],
                marker_line_color='white',
                colorbar_title='Millions USD',
                showscale=False
            ), row=1, col=1)
            fig.add_trace(go.Bar(
                x=black_year['year'],
                y=black_year['count'],
                text=black_year['count'],
                textposition='inside',
                marker_color='chocolate',
                name='Death by year'
            ), row=1, col=2)
            fig.add_trace(go.Histogram(
                x=shoota[shoota[c.RACE] == 'B'][c.AGE],
                nbinsx=10,
                marker_color='brown',
                name='Age Distribution',
                xbins=dict(size=10),
                opacity=1
            ), row=2, col=2)
            fig.update_layout(
                title_text='US Police Killing Black Peoples (2015-2020)',
                title_x=0.5,
                geo=dict(
                    scope='usa',
                    projection=go.layout.geo.Projection(type='albers usa'),
                    showlakes=True,
                    lakecolor='rgb(255, 255, 255)'
                ),
            )
            fig.update_layout(template='plotly_dark', showlegend=False)
            fig.write_image('blacklivesmattersp.png')

    def allrace(self, img_out=False, csv_out=False):
        year_shoot = self.shoots_df.select('year').groupby('year').count().toPandas()
        shoot_state = self.shoots_df.select(c.STATE).groupby(c.STATE).count().toPandas()
        shootb = self.shoots_df.filter(col('race_name') != 'Others')
        only_race = shootb.select('race_name').groupby('race_name').count().toPandas()

        if csv_out:
            self._save_csv(year_shoot, 'year_shoot.csv')
            self._save_csv(shoot_state, 'shoot_state.csv')
            self._save_csv(only_race, 'only_race.csv')

        if img_out:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('Victims in all states', 'Victims by Year', 'Victims by Race',),
                column_widths=[0.6, 0.4],
                row_heights=[0.4, 0.6],
                specs=[
                    [{'type': 'Choropleth', 'rowspan': 2}, {'type': 'bar'}],
                    [None, {'type': 'pie'}]
                ]
            )
            fig.add_trace(go.Choropleth(
                locations=shoot_state[c.STATE],
                z=shoot_state['count'].astype(float),
                locationmode='USA-states',
                colorscale='Reds',
                autocolorscale=False,
                text=shoot_state[c.STATE],
                marker_line_color='white',
                colorbar_title='Millions USD', showscale=False,
            ), row=1, col=1)
            fig.add_trace(go.Bar(
                x=year_shoot['year'],
                y=year_shoot['count'],
                text=year_shoot['count'],
                textposition='inside',
                marker_color='chocolate',
                name='Death by year'
            ), row=1, col=2)
            fig.add_trace(go.Pie(
                labels=only_race['race_name'],
                values=only_race['count'],
                textinfo='percent',
                insidetextorientation='radial'
            ), row=2, col=2)
            fig.update_layout(
                title_text='US Police Killing All Race (2015-2020)',
                title_x=0.5,
                geo=dict(
                    scope='usa',
                    projection=go.layout.geo.Projection(type='albers usa'),
                    showlakes=True,
                    lakecolor='rgb(255, 255, 255)'
                ),
            )
            fig.update_layout(template='plotly_dark')
            fig.write_image('allstateracessp.png')
