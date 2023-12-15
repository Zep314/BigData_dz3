# Вводный курс по Big Data (семинары)

# Урок 3. Инструменты работы и визуализации

import logging
import kaggle
import csv
from functools import reduce
from hdfs import InsecureClient
from contextlib import closing
from pyhive import hive
from collections import defaultdict

HADOOP_URL = '192.168.10.37'
HADOOP_PORT = '50070'
INTERMEDIATE_CSV = 'transformed-data.csv'
HIVE_SERVER = '192.168.10.37'
HIVE_DATABASE = 'default'


# noinspection SpellCheckingInspection
def get_data_from_kaggle():
    # Kaggle token
    # Windows: C:\Users\<user>\.kaggle\kaggle.json
    # Linux: /home/<user>/.kaggle/kaggle.json
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files("CooperUnion/cardataset", path="./", unzip=True, quiet=True)


def transform_data():
    def mapper(filename):
        with open(filename, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                yield row['Make'], int(row['MSRP'])

    def reducer(data1, data2):
        count, total = data1[data2[0]]
        data1[data2[0]] = (count + 1, total + (data2[1] - total) / (count + 1))
        return data1

    initial_data = defaultdict(lambda: (0, 0))  # Начальное значение - tuple (count, total)
    result = reduce(reducer, mapper('data.csv'), initial_data)
    return dict(result)


def save_data_to_csv(local_data):
    with open(f'{INTERMEDIATE_CSV}', 'w', newline='') as state_file:
        writer = csv.writer(state_file)
        writer.writerows([['Make', 'MSRP']] + [[k, v[1]] for k, v in local_data.items()])


def load_to_hadoop():
    client = InsecureClient(f'http://{HADOOP_URL}:{HADOOP_PORT}', user='root')
    client.upload(f'/{INTERMEDIATE_CSV}', f'{INTERMEDIATE_CSV}', overwrite=True)


# noinspection SpellCheckingInspection
def load_to_hive():
    connection = hive.connect(
        host=HIVE_SERVER,
        database=HIVE_DATABASE,
        #        username='root',
        #        password='topsecret',
    )

    with closing(connection):
        cursor = connection.cursor()

        cursor.execute('drop table if exists cardataset')
        cursor.execute("create table cardataset(make String, msrp Float) row format delimited "
                       "fields terminated by ',' stored as textfile")
        cursor.execute(f"load data inpath '/{INTERMEDIATE_CSV}' into table cardataset")


# noinspection SpellCheckingInspection
def data_analysis():
    connection = hive.connect(
        host=HIVE_SERVER,
        database=HIVE_DATABASE,
    )

    with closing(connection):
        cursor = connection.cursor()
        cursor.execute('select make,msrp from cardataset where msrp is not null order by msrp limit 1')
        rows = cursor.fetchall()
        for row in rows:
            print(f'Марка автомобилей с наименьшей средней ценой: {row[0]}, средняя цена: {row[1]}')

        cursor.execute('select make,msrp from cardataset where msrp is not null order by msrp desc limit 1')
        rows = cursor.fetchall()
        for row in rows:
            print(f'Марка автомобилей с наибольшей средней ценой: {row[0]}, средняя цена: {row[1]}')


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.info("Начинаем работу...")
    get_data_from_kaggle()
    data = transform_data()
    save_data_to_csv(data)
    load_to_hadoop()
    load_to_hive()
    data_analysis()
    logger.info("Работа завершена.")
