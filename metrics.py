import pandas as pd
import numpy as np

from config import PATH


class MetricEvaluator:

    def m1(self, csv_file):
        """Computes the rate of increase for crime by year

        input: crimes_trend.csv
        """
        df = pd.read_csv(csv_file, index_col=0)
        df['ratio'] = df['count'].div(df['count'].shift(1)) - 1
        df = df.dropna(subset=['ratio'])[['yearpd', 'ratio']]

        print(df)

    def m2(self, csv_shootings_armed, csv_shootings_unarmed):
        """ Computes and compare the ratio of armed/not armed blacks dead with whites

        input: armed.csv, unarmed.csv
        """
        df_armed = pd.read_csv(csv_shootings_armed, usecols=['race_name', 'armed', 'count'])
        df_unarmed = pd.read_csv(csv_shootings_unarmed, usecols=['race_name', 'armed', 'count'])

        df = pd.concat([df_armed, df_unarmed], axis=0)
        df.reset_index(inplace=True)

        total_blacks = df[df['race_name'] == 'Black']['count'].sum()
        armed_blacks = df_armed[df_armed['race_name'] == 'Black']['count'].values.tolist()[0]
        unarmed_blacks = df_unarmed[df_unarmed['race_name'] == 'Black']['count'].values.tolist()[0]

        total_whites = df[df['race_name'] == 'White']['count'].sum()
        armed_whites = df_armed[df_armed['race_name'] == 'White']['count'].values.tolist()[0]
        unarmed_whites = df_unarmed[df_unarmed['race_name'] == 'White']['count'].values.tolist()[0]

        perc_armed_blacks = armed_blacks / total_blacks
        perc_unarmed_blacks = unarmed_blacks / total_blacks

        perc_armed_whites = armed_whites / total_whites
        perc_unarmed_whites = unarmed_whites / total_whites

        m1 = perc_armed_blacks / perc_armed_whites
        m2 = perc_unarmed_blacks / perc_unarmed_whites

        print(f'Killed: (Armed)B/W: {m1:.2f}   - (Unarmed)B/W: {m2:.2f}')

    def m3(self, csv_crimes_severity):
        """ Computes the ratio of severe crimes on the overall count

        input: crimes_severity.csv
        """
        df_severity = pd.read_csv(csv_crimes_severity)

        years = df_severity['yearpd'].unique()

        data = []
        for year in years:
            year_df = df_severity[df_severity['yearpd'] == year]

            severes = year_df[year_df['LAW_CAT_CD'] == 'FELONY']['count'].values.tolist()[0]
            total = year_df['count'].sum()
            ratio = severes / total
            print(f'Year {year} - Severes: {severes}, Total: {total} --> ratio: {ratio:.2f}')
            data.append([year, ratio])

        df_out = pd.DataFrame(data, columns=['yearpd', 'ratio'])
        print(f'The variance of column ratio is: {df_out["ratio"].var()}')

    def m4(self, csv_crimes_district_race, csv_demo):
        """ Computes the incidence score of an ethnicity on the criminality of the district, normalized by the demo

        input: crimes_districts_race.csv, districts_demo.csv
        """
        df_districts = pd.read_csv(csv_crimes_district_race, index_col=0)
        districts = df_districts['BORO_NM'].unique()

        df_demo = pd.read_csv(csv_demo, index_col=0)

        tabs = []
        for dist in districts:
            df_distr = df_districts[df_districts['BORO_NM'] == dist].copy()
            df_demo_ = df_demo[df_demo['CTYNAME'] == dist].copy()

            df_distr['ratio'] = np.around(df_distr['count']/df_distr['count'].sum(), 2)
            df_demo_['ratio'] = np.around(df_demo_['sum(RESPOP)']/df_demo_['sum(RESPOP)'].sum(), 2)

            df = pd.merge(df_distr, df_demo_, how='left', left_on=['BORO_NM', 'SUSP_RACE'], right_on=['CTYNAME', 'IMPRACE'])

            df = df[['BORO_NM', 'SUSP_RACE', 'ratio_x', 'ratio_y']]
            df['incidence'] = df['ratio_x'] * df['ratio_y']

            tab = pd.pivot_table(df, index=['BORO_NM'], values=['incidence'], columns=['SUSP_RACE'])
            tabs.append(tab)

        out_df = pd.concat(tabs, axis=0)
        print(out_df)

    def m5(self, csv_shootings, csv_police_deaths):
        """ Compare the variation rates of police deaths with blacks deaths

        input: year_shoot.csv, police_deaths.csv
        """
        df_police_deaths = pd.read_csv(csv_police_deaths, index_col=0)
        df_shootings = pd.read_csv(csv_shootings, index_col=0)

        df_police_deaths['ratio'] = df_police_deaths['count'].div(df_police_deaths['count'].shift(1)) - 1
        df_police_deaths = df_police_deaths.dropna(subset=['ratio'])[['year', 'ratio']]

        df_shootings['ratio'] = df_shootings['count'].div(df_shootings['count'].shift(1)) - 1
        df_shootings = df_shootings.dropna(subset=['ratio'])[['year', 'ratio']]

        print('Police Deaths: ')
        print(df_police_deaths)

        print('Blacks Deaths: ')
        print(df_shootings)


def main():
    metrics = MetricEvaluator()
    metrics.m1(f'{PATH}/crimes_trend.csv')
    metrics.m2(f'{PATH}/armed.csv', f'{PATH}/unarmed.csv')
    metrics.m3(f'{PATH}/crimes_severity.csv')
    metrics.m4(f'{PATH}/crimes_districts_race.csv', f'{PATH}/districts_demo.csv')
    metrics.m5(f'{PATH}/year_shoot.csv', f'{PATH}/police_deaths.csv')


if __name__ == '__main__':
    main()
