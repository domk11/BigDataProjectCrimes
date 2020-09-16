from pyspark import StorageLevel
import plotly.graph_objects as go

from src.database.contracts import politics_contract as c
from utils import udf_calculate_winner


class SparkPolitics:

    def __init__(self, politics_df):
        self.politics_df = politics_df
        self._preprocess()

    def _preprocess(self):
        self.politics_df = self.politics_df.withColumn(c.WINNER, udf_calculate_winner(c.REPUBLICAN, c.DEMOCRATIC))
        self.politics_df.persist(StorageLevel.MEMORY_AND_DISK).count()

    def _save_csv(self, df, csv_out):
        df.toPandas().to_csv(csv_out)

    def show_df(self, df, limit=20):
        df.show(limit)

    def polls_map(self, img_out=False, csv_out=False, path=None, cache=False):
        # red republican -> 1, blue democratic -> 0
        politics_df = self.politics_df

        if cache:
            politics_df = politics_df.persist()

        polls_df = politics_df.select([c.STATE, c.WINNER])

        if csv_out:
            self._save_csv(polls_df, f'{path}/us_polls.csv')

        if img_out:
            polls_pd = polls_df.toPandas()

            colors = [[1, 'rgb(255, 0, 0)'], [0, 'rgb(0, 0, 255)']]
            fig = go.Figure(go.Choropleth(
                locations=polls_pd[c.STATE],
                z=polls_pd[c.WINNER].astype(float),
                locationmode='USA-states',
                colorscale=colors,
                autocolorscale=False,
                text=polls_pd[c.STATE],
                marker_line_color='white',
                showscale=False
            ))
            fig.update_layout(
                title_text='US Polls 2016',
                title_x=0.5,
                geo=dict(
                    scope='usa',
                    projection=go.layout.geo.Projection(type='albers usa')
                )
            )
            fig.write_image(f'{path}/us_polls.png')
