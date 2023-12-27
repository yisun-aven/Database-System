import json
import os
from tabulate import tabulate
from helper import compare, is_float


def print_aggregate_result(table, col, records):
    print(tabulate([records], headers=col, colalign=["left" for _ in col]))


def aggregate_count(full_path, table_name, group_by, conditions, condition_type, order_type):
    attributes_dict = {}
    file_names = os.listdir(full_path)
    for file_name in file_names:
        if not file_name.endswith(".json"):
            continue
        file_path = os.path.join(full_path, file_name)
        with open(file_path, 'r') as file:
            table = json.load(file)
            group_by_index = table[table_name]['columns'].index(group_by)
            data = table[table_name]['data']
            for d in data:
                if conditions[0] and condition_type == "AND":
                    if not check_conditions_AND(conditions, d, table[table_name]['columns']):
                        continue
                elif conditions[0] and condition_type == "OR":
                    if check_conditions_OR(conditions, d, table[table_name]['columns']):
                        continue
                value = d[group_by_index]
                if value in attributes_dict:
                    attributes_dict[value] += 1
                else:
                    attributes_dict[value] = 1
    if order_type == "descending":
        attributes_dict = dict(sorted(attributes_dict.items(), key=lambda item: item[1], reverse=True))
    elif order_type == "ascending":
        attributes_dict = dict(sorted(attributes_dict.items(), key=lambda item: item[1]))
    res_columns = list(attributes_dict.keys())
    res_data = list(attributes_dict.values())
    print_aggregate_result(table, res_columns, res_data)


def aggregate_sum(full_path, table_name, group_by, conditions, condition_type, order_type, sum_type):
    # ex:
    # FIND SUM OF price OF category ON shopping IF age > 60 in ascending ORDER
    attributes_dict = {}
    file_names = os.listdir(full_path)
    for file_name in file_names:
        if not file_name.endswith(".json"):
            continue
        file_path = os.path.join(full_path, file_name)
        with open(file_path, 'r') as file:
            table = json.load(file)
            group_by_index = table[table_name]['columns'].index(group_by)
            sum_type_index = table[table_name]['columns'].index(sum_type)
            data = table[table_name]['data']
            for d in data:
                if conditions[0] and condition_type == "AND":
                    if not check_conditions_AND(conditions, d, table[table_name]['columns']):
                        continue
                elif conditions[0] and condition_type == "OR":
                    if check_conditions_OR(conditions, d, table[table_name]['columns']):
                        continue
                group_by_value = d[group_by_index]
                sum_type_value = d[sum_type_index]
                if group_by_value in attributes_dict:
                    attributes_dict[group_by_value] += float(sum_type_value) if is_float(sum_type_value) else sum_type_value
                else:
                    attributes_dict[group_by_value] = float(sum_type_value) if is_float(sum_type_value) else sum_type_value
    if order_type == "descending":
        attributes_dict = dict(sorted(attributes_dict.items(), key=lambda item: item[1], reverse=True))
    elif order_type == "ascending":
        attributes_dict = dict(sorted(attributes_dict.items(), key=lambda item: item[1]))
    res_columns = list(attributes_dict.keys())
    res_data = list(attributes_dict.values())
    print_aggregate_result(table, res_columns, res_data)


def aggregation(directory_name, table_name, aggregate_type, group_by,
                joins, order_type, condition_type, conditions, sum_type):
    # base_path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory_name}/tables/{table_name}"
    current_directory = os.getcwd()
    base_path = os.path.join(current_directory, "myDB", "Database", directory_name, "tables", table_name)
    if joins:
        join_table = joins[0]
        base_path = os.path.join(base_path, f'join_{join_table}')
    if aggregate_type == "count" or aggregate_type == "COUNT":
        aggregate_count(base_path, table_name, group_by, conditions, condition_type, order_type)
    elif aggregate_type == "sum" or aggregate_type == "SUM":
        aggregate_sum(base_path, table_name, group_by, conditions, condition_type, order_type, sum_type)
    else:
        print(f'{aggregate_type} not supported')


def check_conditions_AND(conditions, lst, columns):
    # if d satisfy the conditions:
    for condition in conditions:
        indexCondition = columns.index(condition[0])
        value = lst[indexCondition]
        if is_float(value):
            value = float(value)
        else:
            value = lst[indexCondition]
        if not compare(value, condition[1], condition[2]):
            return False
    return True


def check_conditions_OR(conditions, lst, columns):
    for condition in conditions:
        indexCondition = columns.index(condition[0])
        value = lst[indexCondition]
        if is_float(value):
            value = float(value)
        else:
            value = lst[indexCondition]
        if compare(value, condition[1], condition[2]):
            return True
    return False