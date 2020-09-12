import os
import subprocess
import matplotlib.pyplot as plt

from config import LOCAL_PATH_CENSUS, LOCAL_PATH_CRIME, INPUT_FOLDER, HADOOP_PATH, HADOOP_STREAMING_VERSION


CENSUS_MAPPER_PATH = f'{LOCAL_PATH_CENSUS}/mapper.py'
CENSUS_REDUCER_PATH = f'{LOCAL_PATH_CENSUS}/reducer.py'
INPUT_CENSUS = 'ny_census.csv'
OUTPUT_CENSUS = '/output-census'

CRIME_MAPPER_PATH = f'{LOCAL_PATH_CRIME}/mapper.py'
CRIME_REDUCER_PATH = f'{LOCAL_PATH_CRIME}/reducer.py'
INPUT_CRIME = 'nypd_filtered.csv'
OUTPUT_CRIME = '/output-crime'


def copy_file_hdfs(file):
    os.chdir(HADOOP_PATH)
    command = [
        'bin/hadoop', 'dfs', '-copyFromLocal', file, '/'
    ]
    subprocess.run(command)


def make_executable(file):
    subprocess.run(['chmod', '+x', file])


def run_job(mapper_path, reducer_path, input_path, output_path):
    os.chdir(HADOOP_PATH)
    command = ['bin/hadoop', 'jar', f'contrib/streaming/{HADOOP_STREAMING_VERSION}',
               '-mapper', mapper_path,
               '-reducer', reducer_path,
               '-input', input_path,
               '-output', output_path]
    subprocess.run(command)


def open_results(path):
    return (subprocess.Popen(['hadoop', 'fs', '-cat', path], stdout=subprocess.PIPE).communicate()[0]).decode('utf-8')


def create_dict(results):
    values = filter(lambda s: s != '', results.split('\n'))
    values = [v.split('\t') for v in values]
    return {k: int(v) for (k, v) in values}


def plot_figure(data, filename):
    plt.figure()
    plt.bar(range(len(data)), list(data.values()), align='center')
    plt.xticks(range(len(data)), list(data.keys()))
    plt.savefig(filename)


def process_results():
    census_results = open_results(f'{OUTPUT_CENSUS}/part-00000')
    crime_results = open_results(f'{OUTPUT_CRIME}/part-00000')

    census_dict = create_dict(census_results)
    crime_dict = create_dict(crime_results)

    results = {}
    for census_key, census_value in census_dict.items():
        for crime_key, crime_value in crime_dict.items():
            if census_key == crime_key:
                results[census_key] = (crime_value / census_value) * 100

    plot_figure(census_dict, 'census.png')
    plot_figure(crime_dict, 'crime.png')
    plot_figure(results, 'results.png')


def run():
    copy_file_hdfs(f'{LOCAL_PATH_CENSUS}/{INPUT_FOLDER}/')
    copy_file_hdfs(f'{LOCAL_PATH_CRIME}/{INPUT_FOLDER}/')

    make_executable(CENSUS_MAPPER_PATH)
    make_executable(CENSUS_REDUCER_PATH)
    make_executable(CRIME_MAPPER_PATH)
    make_executable(CRIME_REDUCER_PATH)

    run_job(CENSUS_MAPPER_PATH, CENSUS_REDUCER_PATH, f'{INPUT_FOLDER}/{INPUT_CENSUS}', OUTPUT_CENSUS)
    run_job(CRIME_MAPPER_PATH, CRIME_REDUCER_PATH, f'{INPUT_FOLDER}/{INPUT_CRIME}', OUTPUT_CRIME)

    process_results()


if __name__ == '__main__':
    run()
