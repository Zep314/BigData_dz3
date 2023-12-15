# Вводный курс по Big Data (семинары)

# Урок 3. Инструменты работы и визуализации

import kaggle
import csv
from functools import reduce
from hdfs import InsecureClient

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
                yield (row['Make'], int(row['MSRP']))

    def reducer(data1, data2):
        if not isinstance(data1,dict):
            data1 = {data1[0]: [1, data1[1]]}
        if data2[0] in data1:
            data1[data2[0]][0] += 1
            data1[data2[0]][1] = data1[data2[0]][1] + (data2[1] - data1[data2[0]][1]) / data1[data2[0]][0]
        else:
            data1[data2[0]] = [1, data2[1]]
        return data1

    return reduce(reducer, mapper('data.csv'))


def save_data_to_csv(data):
    with open('transformed-data.csv', 'w', newline='') as state_file:
        writer = csv.writer(state_file)
        writer.writerows([['Make', 'MSRP']] + [[k, v[1]] for k, v in data.items()])


def load_to_hadoop():
    client = InsecureClient('http://192.168.10.37:50070', user='root')
    client.upload('/transformed-data.csv','transformed-data.csv', overwrite=True)

def load_to_hive():
    pass

def data_analysis():
    pass

if __name__ == '__main__':
#    get_data_from_kaggle()
#    data = transform_data()
#    save_data_to_csv(data)
    load_to_hadoop()
    load_to_hive()
    data_analysis()
