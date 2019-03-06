# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from itertools import product
from multiprocessing import Pool, Manager
import math
from datetime import datetime
from itertools import ifilter
import copy
from itertools import islice


def generate_a_series_of_attempts(name, max_volume, times):
    return pd.Series(data=np.linspace(0, max_volume, num=times), name=name)


def generate_map_of_attempts(df):
    (rows, columns) = df.shape
    result = {}
    for i in range(rows):
        row = df.iloc[i]
        name = row[u'信号名称']
        max_volume = row[u'最小手数']
        times = row[u'times']
        result[name] = generate_a_series_of_attempts(name, max_volume, times)
    return result


def calculate_record_size(columns_seeds):
    total = 1

    for s in columns_seeds:
        total *= len(s)

    return total


class SmallTask:
    def __init__(self, no, column_names, column_seeds, default_page_size=100 * 1000):
        self.no = no
        self.column_names = column_names
        self.column_seeds = column_seeds

        self.record_size = calculate_record_size(column_seeds)
        self.page_size = default_page_size
        self.start = 0
        self.stop = self.start + default_page_size

    def update(self):
        self.start = self.stop
        self.stop = self.start + self.page_size

    def isComplete(self):
        return self.start > self.record_size


def generate_initial_small_task_sequence(signal_set, strongest_signal):
    columns = signal_set.keys()
    total_frame = strongest_signal[u'times']
    name_of_strongest_signal = strongest_signal[u'信号名称']
    attempts_of_strongest_signal = signal_set[name_of_strongest_signal].values
    sliced_attempts_of_strongest_signal = [[attempts_of_strongest_signal[i]] for i in range(total_frame)]

    shared_columns = [c for c in ifilter(lambda x: x != name_of_strongest_signal, columns)]

    common_attempts = []
    for c in shared_columns:
        series = signal_set[c].values
        temp = list(series.round(2))
        common_attempts.append(temp)

    frame_sequence = []
    for i in range(total_frame):
        frame_column = copy.copy(shared_columns)
        frame_column.append(name_of_strongest_signal)

        frame_values = copy.copy(common_attempts)
        frame_values.insert(0, sliced_attempts_of_strongest_signal[i])

        frame_sequence.append(SmallTask(i, frame_column, frame_values))

    return frame_sequence


def generate_metadata_for_signals(df):
    df.reset_index(drop=True, inplace=True)

    metadata = {}
    for key, value in df.itertuples(index=False):
        metadata[key] = value
    return metadata


def generate_mapping(relative_df, related_columns=[]):
    if len(related_columns) != 2:
        raise Exception("related_columns should be a string list whose length is two")

    interesting_df = relative_df.loc[:, related_columns]

    decimals = {}
    decimals[related_columns[1]] = 2
    interesting_df = interesting_df.round(decimals)

    return generate_metadata_for_signals(interesting_df)


def calculate_covariance(row, names_of_signals, standard_deviation_of_signals, relationship_df):
    temp_products = {}
    total = 0
    for s in names_of_signals:
        p = row[s] * standard_deviation_of_signals[s]

        temp_products[s] = p
        total += pow(p, 2)

    length = len(names_of_signals)
    for i in range(0, length - 1):
        left = names_of_signals[i]
        for j in range(1, length):
            right = names_of_signals[j]
            total += temp_products[left] * temp_products[right] * 2 * relationship_df.loc[right, left]
    return math.sqrt(total)


def calculate_return(row, names_of_signals, expected_return_of_signals):
    total = 0

    for s in names_of_signals:
        total += row[s] * expected_return_of_signals[s]

    return total


def calculate_multiple(row):
    if row["corelation"] == 0:
        return 0

    return row["balance"] / row["corelation"]


def calculate_sharp_rate(row):
    if row["corelation"] == 0:
        return 0
    return (row["exp_return"] * 12 - row["balance"] * 0.05) / row["corelation"]


def filter(row):
    """
    the function used to filter the result DataFrame
    :param row:
    :return:
    """
    # if row["drawback_ratio"] > 30:
    #     return False

    return True



def extend_columns(target_df, standard_deviation_of_signals, expected_return_of_signals, net_withdrawal_of_signals,
                   relationship_df, principal=19519.18):
    print "begin to extend columns"
    names_of_signals = target_df.columns

    target_df["corelation"] = target_df.apply(calculate_covariance, axis=1,
                                              args=(names_of_signals, standard_deviation_of_signals, relationship_df))
    target_df["exp_return"] = target_df.apply(calculate_return, axis=1, args=(names_of_signals, expected_return_of_signals))
    target_df["drawback"] = target_df.apply(calculate_covariance, axis=1,
                                       args=(names_of_signals, net_withdrawal_of_signals, relationship_df))

    target_df["balance"] = principal
    target_df["times"] = target_df.apply(calculate_multiple, axis=1)
    target_df["drawback_ratio"] = target_df["drawback"] * 100 / target_df["balance"]
    target_df["exp_return_ratio"] = target_df["exp_return"] * 100 / target_df["balance"]
    target_df = target_df.round({"drawback_ratio": 2, "exp_return_ratio": 2})
    target_df["sharp_ratio"] = target_df.apply(calculate_sharp_rate, axis=1)
    print "The extention of columns is complete"
    return target_df


def generate_matrix_of_signals_by(queue, smart_task):
    df = pd.DataFrame(data=islice(product(*smart_task.column_seeds), smart_task.start, smart_task.stop),
                      columns=smart_task.column_names)
    df = df.round(decimals=2)

    smart_task.update()

    if not smart_task.isComplete():
        queue.put(smart_task)

    print "The completion of frame {} : record_size-{}, finished-{}, percentage-{}% " \
        .format(smart_task.no, smart_task.record_size, smart_task.start,
                round(smart_task.start * 1.0 * 100 / smart_task.record_size, 2))
    df.to_csv("temps/temp{}.csv".format(smart_task.no), encoding="utf-8")
    # print "frame {} writing is finished".format(no)

    return df


def predict(queue, smart_task, standard_deviation_of_signals, expected_return_of_signals, net_withdrawal_of_signals,
            relationship_df, principal):
    start_time = datetime.now()

    df = generate_matrix_of_signals_by(queue, smart_task)


    print df.head()

    target_df = extend_columns(df, standard_deviation_of_signals, expected_return_of_signals, net_withdrawal_of_signals,
                               relationship_df, principal=principal)

    filtered_df = target_df.loc[target_df.apply(filter, axis=1), :]

    time_elapsed = datetime.now() - start_time

    print 'Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed)

    return filtered_df


if __name__ == '__main__':
    start_time = datetime.now()
    print "begin to predict ........."

    # 0.load the signal table
    summary_df = pd.read_excel('inputs/signals.xlsx', sheet_name=0, na_values='-')
    # delete unneeded columns
    del summary_df[u'备注']
    summary_df = summary_df.fillna(0)

    # 0.1>list all available signals
    signals = summary_df[u'信号名称'].to_list()
    print "0->all available signals: {}".format(",".join(signals))

    # 1.Load the relation table
    # 1.1> define data types in relation table
    relations_column_types = {}
    for item in signals:
        relations_column_types[item] = np.float32

    # 1.2> loading this relation table
    relationship_df = pd.read_excel('inputs/relations.xlsx', sheet_name=0, index_col=0, dtype=relations_column_types)
    # fill zero for nan
    relationship_df = relationship_df.fillna(0)
    signals_involved = relationship_df.columns.to_list()
    print "1->the relationship among those signals: {}".format(",".join(signals_involved))


    # 2.extract the volume of sales
    # valid_volume_df = summary_df[[u'信号名称', u'最小手数']]
    # valid_volume_df.loc['times'] = valid_volume_df[u'最小手数'].map(lambda x: int(x / 0.01 + 1)) todo test whye

    valid_volume_df = summary_df[[u'信号名称', u'最小手数', u'测试倍数']].copy()
    valid_volume_df.loc[:, u'最小手数'] = valid_volume_df[u'最小手数'].astype(np.float32)
    valid_volume_df.loc[:, u'测试倍数'] = valid_volume_df[u'测试倍数'].astype(np.int16)
    valid_volume_df.loc[:, 'times'] = (valid_volume_df[u'最小手数'] * valid_volume_df[u'测试倍数'] / 0.01 + 1).astype(np.int16)

    # 3.select the desirable signals
    desirable_signals = [u'DM0066', u'CJM995']
    print "3->desired_signals: {}".format(",".join(desirable_signals))

    # 3.1>check whether the signals is valid
    for s in desirable_signals:
        if s not in signals_involved:
            raise Exception("signal: {} is not in relation table".format(s))
        elif s not in signals:
            raise Exception("signal: {} is not available".format(s))
        else:
            print "check process is completed"

    signals_df = valid_volume_df.loc[valid_volume_df[u'信号名称'].map(lambda x: x in desirable_signals)]

    signals_df = signals_df.sort_values(['times'], ascending=False)
    signals_df.reset_index(drop=True, inplace=True)


    # 4.divide the whole work into small tasks
    strongest_signal = signals_df.iloc[0]
    signal_set = generate_map_of_attempts(signals_df)

    standard_deviation_of_signals = generate_mapping(summary_df, related_columns=[u'信号名称', u'标准差'])
    expected_return_of_signals = generate_mapping(summary_df, related_columns=[u'信号名称', u'预期回报'])
    net_withdrawal_of_signals = generate_mapping(summary_df, related_columns=[u'信号名称', u'净值回撤'])

    small_task_sequence = generate_initial_small_task_sequence(signal_set, strongest_signal)

    # 4.1> define the number of cup in use and balance
    cpu_num = 5
    balance = 15083
    print "4->cup in use: {}, current balance: {} ".format(cpu_num, balance)

    p = Pool(cpu_num)
    m = Manager()
    queue = m.Queue(len(small_task_sequence) + cpu_num)

    for st in small_task_sequence:
        queue.put(st)

    async_result_set = []
    result_set = []
    while True:

        if len(async_result_set) < cpu_num:
            st = queue.get()

            temp = p.apply_async(predict, (
                queue, st, standard_deviation_of_signals, expected_return_of_signals, net_withdrawal_of_signals,
                relationship_df, balance))

            async_result_set.append(temp)
        else:

            ar = async_result_set.pop(0)
            result_set.append(ar.get())

        if queue.empty():
            break

    p.close()
    p.join()


    # 6.merge all the results
    final_df = pd.concat([r.get() for r in async_result_set], ignore_index=True)
    final_df.to_csv("outputs/final_prediction.csv", encoding="utf-8", float_format="%.2f")
    time_elapsed = datetime.now() - start_time
    print "Prediction is completed ........."
    print 'Time elapsed (hh:mm:ss.ms) {}'.format(time_elapsed)
