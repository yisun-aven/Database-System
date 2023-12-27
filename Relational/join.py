import os
import json
import itertools
from tabulate import tabulate
from helper import compare, is_float
from external_sort import external_sort_and_write


def load_sorted_chunk(file, table_name, key):
    loaded_data = json.load(file)
    data = loaded_data[table_name]["data"]
    key_index = loaded_data[table_name]['columns'].index(key)
    return iter(sorted(data, key=lambda x: x[key_index])), loaded_data, key_index


def sort_merge_join(iter1, iter2, key1_index, key2_index):
    iter1 = list(iter1)
    iter2 = list(iter2)
    i = j = 0
    while i < len(iter1) and j < len(iter2):
        if iter1[i][key1_index] == iter2[j][key2_index]:
            yield iter1[i], iter2[j]
            i += 1
            j += 1
        elif iter1[i][key1_index] < iter2[j][key2_index]:
            i += 1
        else:
            j += 1


def join_data(directory_name, table1_name, table2_name, key1, key2, conditions, condition_type,
              order_type, order_attribute, order_attribute_index, attributes):
    current_directory = os.getcwd()
    path = os.path.join(current_directory, "myDB", "Database", directory_name, "tables")
    # path = f"/Users/Aven/Desktop/myDB/Database/{directory_name}/tables"
    full_path1 = os.path.join(path, table1_name)
    full_path2 = os.path.join(path, table2_name)
    if not os.path.exists(os.path.join(full_path1, f'join_{table2_name}')):
        os.mkdir(os.path.join(full_path1, f'join_{table2_name}'))
        chunk_size = 1000  # Define your chunk size based on your memory constraints
        chunk = []
        file_count = 0
        for file1 in os.listdir(full_path1):
            if not file1.endswith(".json"):
                continue
            for file2 in os.listdir(full_path2):
                if not file2.endswith(".json"):
                    continue
                with open(os.path.join(full_path1, file1), 'r') as f1:
                    iter1, loaded_data1, key1_index = load_sorted_chunk(f1, table1_name, key1)
                with open(os.path.join(full_path2, file2), 'r') as f2:
                    iter2, loaded_data2, key2_index = load_sorted_chunk(f2, table2_name, key2)
                    for row in sort_merge_join(iter1, iter2, key1_index, key2_index):
                        lst = sum(row, [])
                        columns1 = loaded_data1[table1_name]['columns']
                        columns2 = loaded_data2[table2_name]['columns']
                        chunk.append(lst)
                        if len(chunk) >= chunk_size:
                            p = os.path.join(full_path1, f'join_{table2_name}')
                            file_path = os.path.join(p, f'results_{file_count}.json')
                            loaded_data = loaded_data1.copy()
                            loaded_data[table1_name]['columns'] = columns1 + columns2
                            loaded_data[table1_name]['data'] = chunk
                            with open(file_path, 'w') as f:
                                json.dump(loaded_data, f)
                            chunk = []
                            file_count += 1

            if chunk:  # Write the last chunk if it's not empty
                # sorted_chunk = sort_data(chunk, order_attribute_index)  # Sort the chunk
                p = os.path.join(full_path1, f'join_{table2_name}')
                file_path = os.path.join(p, f'results_{file_count}.json')
                loaded_data = loaded_data1.copy()
                loaded_data[table1_name]['columns'] = columns1 + columns2
                loaded_data[table1_name]['data'] = chunk
                with open(file_path, 'w') as f:
                    json.dump(loaded_data, f)

    p = os.path.join(full_path1, f'join_{table2_name}')
    if not order_type:
        print_join_res(p, condition_type, conditions, table1_name, order_type, order_attribute_index, attributes)
    else:
        external_sort_and_write(p, table1_name, directory_name, order_type, order_attribute, order_attribute_index)
        p = os.path.join(p, f'sorted_{table1_name}/{order_attribute}')
        print_join_res(p, condition_type, conditions, table1_name, order_type, order_attribute_index, attributes)


def print_join_res(full_path, condition_type, conditions, table_name, order_type, order_attribute_index, attributes):
    res_list = []
    file_names = os.listdir(full_path)
    if order_type:
        if order_type == "descending":
            file_names = sorted(file_names, key=lambda x: x[order_attribute_index], reverse=True)
        else:
            file_names = sorted(file_names)
    else:
        file_names = sorted(file_names)

    for file_name in file_names:
        if not file_name.endswith(".json"):
            continue
        path = os.path.join(full_path, file_name)
        with open(path, 'r') as file:
            table = json.load(file)
            columns = table[table_name]['columns']
            data = table[table_name]['data']
            for lst in data:
                if condition_type == "AND":
                    if check_conditions_AND(conditions, lst, columns):
                        col, lst = select_attributes(table_name, table, lst, attributes)
                        res_list.append(lst)
                elif condition_type == "OR":
                    if check_conditions_OR(conditions, lst, columns):
                        col, lst = select_attributes(table_name, table, lst, attributes)
                        res_list.append(lst)
                if order_type == "descending":
                    res_list = sorted(res_list, key=lambda x: x[order_attribute_index], reverse=True)
                if len(res_list) >= 50:
                    print(tabulate(res_list, headers=col, colalign=["left" for _ in col]))
                    res_list = []
                    continue_data = input(
                        f"There are more matching data. Expand and see more? (yes/no): ").strip().lower()
                    if continue_data == "no":
                        return
    if res_list:
        print(tabulate(res_list, headers=col, colalign=["left" for _ in col]))



def check_conditions_AND(conditions, lst, columns):
    # if d satisfy the conditions:
    if not conditions or not conditions[0]:
        return True
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
    if not conditions or not conditions[0]:
        return True
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


def select_attributes(table_name, table, data, output_col_name):
    newData = []
    if "everything" in output_col_name:
        output_col_name = table[table_name]['columns']

    for col in output_col_name:
        indexOutput = table[table_name]['columns'].index(col)
        # I am only keeping the attributes at indexOutput
        newData.append(data[indexOutput])
    return output_col_name, newData
