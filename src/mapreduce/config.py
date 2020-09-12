# extract in csv the two collection 'ny_census' and 'nypd_filtered'
# ny_census.csv and nypd_filtered.csv must be saved inside {LOCAL_PATH}/{INPUT_FOLDER}

LOCAL_PATH = '/home/marco'
LOCAL_PATH_CENSUS = f'{LOCAL_PATH}/mapreduce_census'
LOCAL_PATH_CRIME = f'{LOCAL_PATH}/mapreduce_crime'

INPUT_FOLDER = '/input'

HADOOP_PATH = '/usr/local/hadoop'
HADOOP_STREAMING_VERSION = 'hadoop-streaming-1.0.4.jar'
