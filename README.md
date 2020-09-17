# USA: equal justice under law, or maybe not?

## Datasets
* Census: https://www2.census.gov/programs-surveys/popest/datasets/2010/modified-race-data-2010/stco-mr2010_mt_wy.csv
* New York crimes: https://data.cityofnewyork.us/api/views/qgea-i56i/rows.csv?accessType=DOWNLOAD
* Washington Post shootings: https://github.com/domk11/BigDataProjectCrimes/blob/master/shootings_wash_post.csv
* Police Deaths: https://github.com/domk11/BigDataProjectCrimes/blob/master/police_killings.csv
* Presidential Elections (2016): https://github.com/domk11/BigDataProjectCrimes/blob/master/presidential_polls_2016.csv

## Steps to perform the analysis

1) Load the datasets inside a database named 'datascience' respecting the collection names described as depicted in contracts.
Create an output folder as described into settings file.


2) Run dataset filtering and cleaning

```python
    python filter_original_dataset.py
```

3) Run New York crime analysis

```python
    python crimes_type.py
```

4) Run Washington post killings analysis

```python
    python shoots.py
```

5) Run Presidential elections analysis 

```python
    python polls.py
```

6) Run Police Deaths analysis

```python
    python police_deaths.py
```

7) Run Districts Census

```python
    python pop_distribution.py
```

8) Census analysis with mapreduce

Into src/mapreduce

```python
    python run.py
```
