import pandas as pd
import numpy as np

class MetricEvaluator:

    def m1(self, csv_file):
        """ Computes the rate of increase for crime by year """
        """ inputs: crimes_trend.csv """

        df = pd.read_csv(csv_file, index_col=0)
        df['ratio %'] = df['count'].div(df['count'].shift(1)) - 1
        df = df.dropna(subset=['ratio'])[['yearpd', 'ratio']]

        print(df)

    def m2(self, csv_shootings):
        """ Computes and compare the ratio of armed/not armed blacks dead with whites """
        """ input: crimes_districts_race.csv """

        df_shootings = pd.read_csv(csv_shootings, index_col=0)

        # TODO

    def m3(self, csv_crimes_severity):
        """ Computes the ratio of severe crimes on the overall count """
        """ input: crimes_severity.csv """

        df_severity = pd.read_csv(csv_crimes_severity)

        years = df_severity['yearpd'].unique()

        data = []
        for year in years:
            year_df = df_severity[df_severity['yearpd'] == year]

            severes = year_df[year_df['LAW_CAT_CD'] == 'FELONY']['count'].values.tolist()[0]
            total = year_df['count'].sum()
            ratio = severes / total
            print(f"Year {year} - Severes: {severes}, Total: {total} --> ratio: %.2f" % (ratio))
            data.append([year, ratio])

        df_out = pd.DataFrame(data, columns=['yearpd', 'ratio'])
        print("The variance of column ratio is: ", df_out['ratio'].var())

    def m4(self, csv_crimes_district_race, csv_demo):
        """ Computes the incidence score of an ethnicity on the criminality of the district, normalized by the demo """
        """ input: crimes_districts_race.csv, districts_demo.csv """

        df_districts = pd.read_csv(csv_crimes_district_race, index_col=0)
        distiricts = df_districts['BORO_NM'].unique()

        df_demo = pd.read_csv(csv_demo, index_col=0)

        tabs = []
        for dist in distiricts:
            df_distr = df_districts[df_districts['BORO_NM'] == dist].copy()
            df_demo_ = df_demo[df_demo['CTYNAME'] == dist].copy()

            df_distr['ratio'] = np.around(df_distr['count']/df_distr['count'].sum(), 2)
            df_demo_['ratio'] = np.around(df_demo_['sum(RESPOP)']/df_demo_['sum(RESPOP)'].sum(), 2)

            df = pd.merge(df_distr, df_demo_, how='left', left_on=['BORO_NM', 'SUSP_RACE'], right_on=['CTYNAME', 'IMPRACE'])

            df = df[['BORO_NM','SUSP_RACE', 'ratio_x', 'ratio_y']]
            df['incidence'] = df['ratio_x'] * df['ratio_y']

            tab = pd.pivot_table(df, index=['BORO_NM'], values=['incidence'], columns=['SUSP_RACE'])
            tabs.append(tab)

        out_df = pd.concat(tabs, axis=0)
        print(out_df)

    def m5(self, csv_shootings, csv_police_deaths):
        pass


def main():

    output_path = '/home/marco/output/'

    metrics = MetricEvaluator()
    # metrics.m1(output_path + 'crimes_trend.csv')

    # metrics.m3(output_path + 'crimes_severity.csv')

    metrics.m4(output_path + 'crimes_districts_race.csv', output_path + 'districts_demo.csv')


if __name__ == '__main__':
    main()