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


logging.basicConfig(format='%(asctime)s.%(msecs)03d :%(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)


def get_data_from_kaggle():
    """
    Kaggle token
    Windows: C:/Users/<user>/.kaggle/kaggle.json
    Linux: /home/<user>/.kaggle/kaggle.json
    Загружаем датасет из Kaggle. В текущем каталоге ожидается файл data.csv (такой dataset)
    :return:
    """
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files('CooperUnion/cardataset', path='./', unzip=True, quiet=True)


def transform_data():
    """
    Считываем файл data.csv из корневой папки, обрабатываем его построчно (функция mapper),
    считаем статистику средней цены автомобиля, группируя по их производителям (функция reducer).
    :return: Словарь с данными (Производитель автомобилей - средняя цена)
    """
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
    """
    Записываем входные данные в csv файл в текущий каталог на диске
    :param local_data:
    :return:
    """
    with open(f'{INTERMEDIATE_CSV}', 'w', newline='') as state_file:
        writer = csv.writer(state_file)
        writer.writerows([['Make', 'MSRP']] + [[k, v[1]] for k, v in local_data.items()])


def load_to_hadoop():
    """
    Передаем файл из текущего каталога в корневую папку HADOOP
    :return:
    """
    client = InsecureClient(f'http://{HADOOP_URL}:{HADOOP_PORT}', user='root')
    client.upload(f'/{INTERMEDIATE_CSV}', f'{INTERMEDIATE_CSV}', overwrite=True)


# noinspection SpellCheckingInspection
def load_to_hive():
    """
    Создаем таблицу в HIVE, и грузим в нее данные из файла, находщегося в корневой папке HADOOP
    :return:
    """
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
    """
    Соединяемся с HIVE, выполняем SQL запросы, собираем аналитику
    :return:
    """
    connection = hive.connect(
        host=HIVE_SERVER,
        database=HIVE_DATABASE,
    )

    with closing(connection):
        cursor = connection.cursor()
        cursor.execute('select make,msrp from cardataset where msrp is not null order by msrp limit 1')
        rows = cursor.fetchall()
        for row in rows:
            logging.info(f'Марка автомобилей с наименьшей средней ценой: {row[0]}, средняя цена: {row[1]}')

        cursor.execute('select make,msrp from cardataset where msrp is not null order by msrp desc limit 1')
        rows = cursor.fetchall()
        for row in rows:
            logging.info(f'Марка автомобилей с наибольшей средней ценой: {row[0]}, средняя цена: {row[1]}')


if __name__ == '__main__':
    logging.info("Начинаем работу...")

    logging.info("Получаем данные из Kaggle...")
    get_data_from_kaggle()
    logging.info("Данные из Kaggle получены.")

    logging.info("Обработка данных. Считаем среднюю цену автомобилей по их маркам...")
    data = transform_data()
    logging.info("Обработка данных завершена.")

    logging.info("Записываю данные в csv файл...")
    save_data_to_csv(data)
    logging.info("Данные записаны в csv файл.")

    logging.info("Передаю csv файл в HADOOP(hdfs)...")
    load_to_hadoop()
    logging.info("Файл передан в HADOOP (hdfs).")

    logging.info("Загрузка данных из фала на hdfs в HIVE...")
    load_to_hive()
    logging.info("Данные помещены в HIVE.")

    logging.info("Запуск аналитических функций в HIVE...")
    data_analysis()
    logging.info("Обработка данных завершена.")

    logging.info("Работа завершена.")

# Результат работы программы:
# /home/user/Work/Python/BigData_dz3/.venv/bin/python /home/user/Work/Python/BigData_dz3/main.py
# 2023-12-15 22:25:25.851 :INFO Начинаем работу...
# 2023-12-15 22:25:25.851 :INFO Получаем данные из Kaggle...
# 2023-12-15 22:25:27.823 :INFO Данные из Kaggle получены.
# 2023-12-15 22:25:27.823 :INFO Обработка данных. Считаем среднюю цену автомобилей по их маркам...
# 2023-12-15 22:25:28.131 :INFO Обработка данных завершена.
# 2023-12-15 22:25:28.131 :INFO Записываю данные в csv файл...
# 2023-12-15 22:25:28.132 :INFO Данные записаны в csv файл.
# 2023-12-15 22:25:28.133 :INFO Передаю csv файл в HADOOP(hdfs)...
# 2023-12-15 22:25:28.133 :INFO Instantiated <InsecureClient(url='http://192.168.10.37:50070')>.
# 2023-12-15 22:25:28.134 :INFO Uploading 'transformed-data.csv' to '/transformed-data.csv'.
# 2023-12-15 22:25:28.135 :INFO Listing '/transformed-data.csv'.
# 2023-12-15 22:25:28.235 :INFO Writing to '/transformed-data.csv'.
# 2023-12-15 22:25:28.406 :INFO Файл передан в HADOOP (hdfs).
# 2023-12-15 22:25:28.406 :INFO Загрузка данных из фала на hdfs в HIVE...
# 2023-12-15 22:25:28.847 :INFO USE `default`
# 2023-12-15 22:25:29.036 :INFO drop table if exists cardataset
# 2023-12-15 22:25:29.219 :INFO create table cardataset(make String, msrp Float) row format delimited fields terminated by ',' stored as textfile
# 2023-12-15 22:25:29.395 :INFO load data inpath '/transformed-data.csv' into table cardataset
# 2023-12-15 22:25:29.840 :INFO Данные помещены в HIVE.
# 2023-12-15 22:25:29.841 :INFO Запуск аналитических функций в HIVE...
# 2023-12-15 22:25:30.155 :INFO USE `default`
# 2023-12-15 22:25:30.330 :INFO select make,msrp from cardataset where msrp is not null order by msrp limit 1
# 2023-12-15 22:25:32.670 :INFO Марка автомобилей с наименьшей средней ценой: Plymouth, средняя цена: 3122.9023
# 2023-12-15 22:25:32.717 :INFO select make,msrp from cardataset where msrp is not null order by msrp desc limit 1
# 2023-12-15 22:25:34.424 :INFO Марка автомобилей с наибольшей средней ценой: Bugatti, средняя цена: 1757223.6
# 2023-12-15 22:25:34.759 :INFO Обработка данных завершена.
# 2023-12-15 22:25:34.759 :INFO Работа завершена.
#
# Process finished with exit code 0
