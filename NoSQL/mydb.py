# mydb.py
from file_handler import FileHandler
from tabulate import tabulate
import json
from helper import compare, is_float
import os
import shutil
import time
import ijson


class MyDB:
    def __init__(self):
        # Initialize database-related attributes and structures here
        # self.tables = {}
        # table size is limited (main memory size)?
        # if table size is full, we need to clear the table and load the other chunks?
        self.tables = {"table_names": []}
        self.current_table = {}  # max size should be 10 rows
        self.table_names = []
        self.mode = "read"

    def unique_file_name(self):
        # Generate a timestamp (Unix timestamp)
        timestamp = int(time.time())
        pid = os.getpid()
        # Combine the timestamp and additional information
        unique_id = f"{timestamp}_{pid}"
        return unique_id

    def save_metadata(self, directory_name):
        # This functions should store all the tables' name and other relevant information
        # it is okay to rewrite this everytime
        # full_path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory_name}"
        current_directory = os.getcwd()
        full_path = os.path.join(current_directory, "myDB", "Database", directory_name)
        self.tables['table_names'] = self.table_names
        data = {
            'tables': self.tables
        }
        json_data = json.dumps(data)
        path = os.path.join(full_path, f'{directory_name}MetaData')
        with open(path, 'w') as file:
            file.write(json_data)
        print(f"Saved MetaData to {directory_name}")

    def load_metadata(self, directory_name):
        # This functions should load all the tables' name and other relevant information
        # full_path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory_name}"
        current_directory = os.getcwd()
        full_path = os.path.join(current_directory, "myDB", "Database", directory_name)
        path = os.path.join(full_path, f'{directory_name}MetaData')
        json_data = ""
        with open(path, 'r') as file:
            for line in file:
                json_data += line
        data = json.loads(json_data)
        # Update the table_names attribute
        if 'tables' in data and 'table_names' in data['tables']:
            self.table_names = data['tables']['table_names']
            self.tables['table_names'] = self.table_names
        print(f"Load MetaData From {directory_name}")

    def save_table(self, directory_name):  # anytime we modify a table
        # save the current table into individual files
        current_directory = os.getcwd()
        table_name = list(self.current_table.keys())[0]
        path = os.path.join(current_directory, "myDB", "Database", directory_name, "tables")
        # path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory_name}/tables"

        count = 0
        table_directory = os.listdir(path)
        full_path = os.path.join(current_directory, "myDB", "Database", directory_name, "tables", table_name)
        # full_path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory_name}/tables/{table_name}"
        if table_name in table_directory:
            filenames = os.listdir(full_path)
            count = len(filenames)
        else:
            os.mkdir(full_path)

        # check if the last file in the table_name directory has data size > 50
        if count > 0:
            filename = filenames[count - 1]
            path = os.path.join(full_path, filename)
            with open(path, 'r') as file:
                loaded_data = json.load(file)
            table_size = len(loaded_data[table_name]['data'])
            if table_size < 50:
                loaded_data[table_name]['data'].extend(self.current_table[table_name]['data'])
                data = json.dumps(loaded_data)
                with open(path, 'w') as file:
                    file.write(data)
                return
        path = os.path.join(full_path, f'{table_name + str(count + 1)}.json')
        data = self.current_table
        json_data = json.dumps(data)
        # should not modify the entire table, but for now, leave it as it is
        with open(path, 'w') as file:
            file.write(json_data)
        print(f"Saved {table_name} to {table_name + str(count + 1)}.json")

    def load_table(self, table_name, directory_name):
        # should only load max 10 rows of data
        current_directory = os.getcwd()
        # full_path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory_name}/tables/{table_name}"
        full_path = os.path.join(current_directory, "myDB", "Database", directory_name, "tables", table_name)
        try:
            filename = os.listdir(full_path)
        except:
            print("No such file or directory")
            return False
        found_data = False
        json_data = ""
        for f in sorted(filename):
            if f.endswith(".json"):
                path = os.path.join(full_path, f)

                with open(path, 'r') as file:
                    for line in file:
                        if "data" in line:
                            index = line.index("data") + 5
                            json_data += line[:index + 1]

                            data = json.loads(f"{json_data}" + " []}}")

                            found_data = True
                            break
                        else:
                            json_data += line
                    if found_data:
                        file.close()
                if found_data:
                    break

        self.form_table(table_name, data, directory_name)

    def form_table(self, table_name, data, directory_name):
        self.current_table.clear()
        self.current_table = data
        ten_data = self.limit_data(table_name, directory_name)
        self.current_table[table_name]['data'] = ten_data

    def limit_data(self, table_name, directory_name):
        # full_path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory_name}/tables/{table_name}"
        current_directory = os.getcwd()
        full_path = os.path.join(current_directory, "myDB", "Database", directory_name, "tables", table_name)
        data = []
        filenames = os.listdir(full_path)
        try:
            for f in sorted(filenames):
                if f.endswith(".json"):
                    path = os.path.join(full_path, f)

                    with open(path, 'r') as file:
                        objects = ijson.items(file, f'{table_name}.data.item')
                        for row in objects:
                            data.append(row)
                            if len(data) == 10:

                                return data
                                break
            return data
        except FileNotFoundError:
            print("No such file or directory")
            return None

    def create_table(self, table_name, columns, pkey, datatype):
        # Implement the logic to create a table
        # print(self.tables)
        if not pkey:
            print("Table creation failed: A primary key is needed!")
            return
        if table_name not in self.table_names:
            self.current_table.clear()
            self.current_table[table_name] = {
                'columns': columns,
                'type': datatype,
                'primary key': pkey,
                'data': []
                # 'index': {}
            }

            self.table_names.append(table_name)
            self.mode = "write"

            print(f"Table '{table_name}' created.")
        else:
            print(f"Table '{table_name}' already exists.")
            return

    def show_table(self, table_name):
        if table_name in self.table_names:
            columns = self.current_table[table_name]['columns']
            data = self.get_table_data(table_name)  # Implement get_table_data() in MyDB class
            # Create a list of column alignments (left-aligned for all columns)
            column_alignments = ["left" for _ in columns]
            if data:
                print(tabulate(data, headers=columns, colalign=column_alignments))
            else:
                print(tabulate(data, headers=columns))
            print("Total Rows:", len(data))
        else:
            print(f"Table '{table_name}' does not exist.")
            return

    def show_tables(self):
        if len(self.table_names) > 0 and self.table_names[0] != "":
            for table_name in self.table_names:
                print(table_name)
        else:
            print("The database is empty")

    def get_table_data(self, table_name):
        # Retrieve data from a table
        if table_name in self.current_table:
            return self.current_table[table_name]['data']
        else:
            raise ValueError(f"Table '{table_name}' does not exist.")

    def execute_query(self, query):
        # Implement the logic to execute a query
        # You can parse and process the query here
        pass

    def insert_into(self, table_name, data, directory_name):
        if table_name in self.table_names:
            self.mode = "write"
            table = self.current_table[table_name]
            table['data'] = []

            if table['columns']:
                if len(data) != len(table['columns']):
                    raise print("Number of values doesn't match the number of columns.")

                if "primary key" in table.keys():
                    primary_key = self.current_table[table_name]['primary key']
                    primary_key_index = self.current_table[table_name]['columns'].index(primary_key)
                    if self.check_duplicate_primary_key(data, primary_key, primary_key_index, directory_name, table_name):
                        # key already exist
                        return
                table['data'].append(data)
                self.save_table(directory_name)
                return
            # need to save immediately to the database
            # self.save_table()
        else:
            raise print(f"Table '{table_name}' does not exist.")

    def check_duplicate_primary_key(self, data, primary_key, primary_key_index, directory_name, table_name):
        # full_path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory_name}/tables/{table_name}"
        current_directory = os.getcwd()
        full_path = os.path.join(current_directory, "myDB", "Database", directory_name, "tables", table_name)
        filenames = os.listdir(full_path)
        for f in sorted(filenames):
            path = os.path.join(full_path, f)

            with open(path, 'r') as file:
                objects = ijson.items(file, f'{table_name}.data.item')
                # objects = ijson.items(file, 'example.data', multiple_values=True)
                for row in objects:
                    if data[primary_key_index] == row[primary_key_index]:
                        print(f"Cannot have duplicate keys: Key {data[primary_key_index]} is already in the table.")
                        return True
        return False

    def drop_table(self, table_name, directory_name):
        # find the table in which file, and then erase the table, then rewrite the file
        if table_name in self.table_names:
            self.mode = "write"
            self.table_names.remove(table_name)
            # full_path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory_name}/tables/{table_name}"
            current_directory = os.getcwd()
            full_path = os.path.join(current_directory, "myDB", "Database", directory_name, "tables", table_name)
            # Check if the directory exists
            if os.path.exists(full_path):
                # Use shutil.rmtree to delete the directory
                shutil.rmtree(full_path)
                print(f"Table '{table_name}' has been dropped.")
            else:
                print(f"Directory '{full_path}' does not exist.")
        else:
            print(f"Table '{table_name}' does not exist.")

    def find_records(self, attributes, conditions, condition_type, table_name, directory_name, order_type, order_attribute):
        total_count = 0
        headers_printed = False
        noConditionSpecified = False
        res = []
        self.load_table(table_name, directory_name)
        current_directory = os.getcwd()
        if not order_type:
            # path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory_name}/tables/{table_name}"
            path = os.path.join(current_directory, "myDB", "Database", directory_name, "tables", table_name)
        else:
            # p = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory_name}/tables/{table_name}/sorted_{table_name}"
            p = os.path.join(current_directory, "myDB", "Database", directory_name, "tables", table_name,
                             f"sorted_{table_name}")
            path = os.path.join(p, f'{order_attribute}')

        if table_name in self.table_names:
            filenames = os.listdir(path)

            if order_type:
                if order_type == "descending":
                    filenames = sorted(filenames, reverse=True)
                else:
                    filenames = sorted(filenames)
            else:
                filenames = sorted(filenames)

            for file in filenames:
                if not file.endswith(".json"):
                    continue
                full_path = os.path.join(path, file)
                with open(full_path, 'r') as f:
                    loaded_data = json.load(f)  # each file is guaranteed to not exceed main memory

                    if conditions and conditions[0]:
                        if condition_type == "AND":
                            data = self.check_conditions_AND(loaded_data, table_name, [], conditions)
                        elif condition_type == "OR":
                            data = self.check_conditions_OR(loaded_data, table_name, [], conditions)
                    else:
                        # if no condition specified
                        if not order_type:
                            # data = []
                            # noConditionSpecified = True
                            data = loaded_data[table_name]['data']
                        else:

                            data = loaded_data[table_name]['data']

                if order_type == "descending":
                    data = sorted(data, key=lambda x: float(x[loaded_data[table_name]['columns'].index(order_attribute)]),
                                  reverse=True)

                self.current_table[f'{table_name}']['data'] = data

                # select which attributes/columns to output
                data = self.select_attributes(table_name, data, attributes)
                curr_count = len(data)
                total_count += curr_count
                if "everything" in attributes:
                    attributes = self.current_table[table_name]['columns']

                if data:
                    if len(data) > 50 or len(res) > 50:
                        while data:
                            res.extend(data[0:50])
                            print(tabulate(res, headers=attributes, colalign=["left" for _ in attributes]))
                            continue_data = input(
                                f"There are more matching data. Expand and see more? (yes/no): ").strip().lower()
                            if continue_data == "no":
                                return
                            data = data[50:]
                            res = []
                    else:
                        res.extend(data)

                if noConditionSpecified:
                    return
            if res:
                print(tabulate(res, headers=attributes, colalign=["left" for _ in attributes]))
            print("Total Rows:", total_count)
        else:
            print(f"Table '{table_name}' does not exist.")
            return

    def check_conditions_AND(self, table, table_name, datalist, conditionlist):
        for d in table[f'{table_name}']['data']:
            conditionNotSatisfied = False
            # if d satisfy the conditions:
            for condition in conditionlist:
                indexCondition = table[f'{table_name}']['columns'].index(condition[0])
                value = d[indexCondition]
                if is_float(value):
                    value = float(value)
                else:
                    value = d[indexCondition]
                if not compare(value, condition[1], condition[2]):
                    conditionNotSatisfied = True
                    break
            if not conditionNotSatisfied:
                datalist.append(d)
        return datalist

    def check_conditions_OR(self, table, table_name, datalist, conditionlist):
        for d in table[table_name]['data']:
            # if d satisfy the conditions:
            for condition in conditionlist:
                indexCondition = table[f'{table_name}']['columns'].index(condition[0])
                value = d[indexCondition]
                if is_float(value):
                    value = float(value)
                else:
                    value = d[indexCondition]
                if compare(value, condition[1], condition[2]):
                    if d not in datalist:
                        datalist.append(d)
        return datalist

    def select_attributes(self, table_name, data, output_col_name):
        newData = [[] for _ in range(len(data))]
        if "everything" in output_col_name:
            output_col_name = self.current_table[table_name]['columns']

        for col in output_col_name:
            indexOutput = self.current_table[table_name]['columns'].index(col)
            # I am only keeping the attributes at indexOutput
            for i, d in enumerate(data):
                newData[i].append(d[indexOutput])
        return newData

    def delete_records(self, conditions, condition_type, table_name, directory_name):
        self.load_table(table_name, directory_name)
        # path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory_name}/tables/{table_name}"
        current_directory = os.getcwd()
        path = os.path.join(current_directory, "myDB", "Database", directory_name, "tables", table_name)
        if table_name in self.table_names:
            filenames = os.listdir(path)
            for file in filenames:
                if not file.endswith(".json"):
                    continue
                full_path = os.path.join(path, file)
                with open(full_path, 'r') as f:
                    loaded_data = json.load(f)
                    if conditions and conditions[0]:
                        if condition_type == "AND":
                            data = self.check_conditions_AND(loaded_data, table_name, [], conditions)
                        elif condition_type == "OR":
                            data = self.check_conditions_OR(loaded_data, table_name, [], conditions)
                    else:
                        data = self.get_table_data(table_name)
                # Delete the records that meet the conditions
                for record in data:
                    loaded_data[f'{table_name}']['data'].remove(record)
                # Write the updated data back to the file
                with open(full_path, 'w') as f:
                    json.dump(loaded_data, f)
        else:
            print(f"Table '{table_name}' does not exist.")
            return

    def update_records(self, values, conditions, condition_type, table_name, directory_name):
        self.load_table(table_name, directory_name)
        # path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory_name}/tables/{table_name}"
        current_directory = os.getcwd()
        path = os.path.join(current_directory, "myDB", "Database", directory_name, "tables", table_name)
        updated_data_count = 0
        if table_name in self.table_names:
            filenames = os.listdir(path)
            for file in filenames:
                if not file.endswith(".json"):
                    continue
                full_path = os.path.join(path, file)
                with open(full_path, 'r') as f:
                    loaded_data = json.load(f)
                    if conditions and conditions[0]:
                        if condition_type == "AND":
                            data = self.check_conditions_AND(loaded_data, table_name, [], conditions)
                        elif condition_type == "OR":
                            data = self.check_conditions_OR(loaded_data, table_name, [], conditions)
                    else:
                        data = self.get_table_data(table_name)

                updated_data_count += len(data)
                # Update the records that meet the conditions
                for record in data:
                    index = self.current_table[table_name]['columns'].index(values[0])
                    record[index] = values[2]
                # Write the updated data back to the file
                with open(full_path, 'w') as f:
                    json.dump(loaded_data, f)
            print("Numbers of data updated:", updated_data_count)
        else:
            print(f"Table '{table_name}' does not exist.")
            return





