# cli.py
from mydb import MyDB
from file_handler import FileHandler
import json
from tabulate import tabulate
from helper import type_mapping, CRUDSet, SystemSet
from external_sort import external_sort_and_write, load_sorted_block
import join
from aggregation import aggregation
import os


class MyDBCLI:
    def __init__(self):
        # Initialize the CLI interface and any necessary attributes
        self.mydb = MyDB()
        self.file_handler = FileHandler()
        self.directory = None

    def set_directory(self, directory_path):
        self.directory = directory_path  # Set the directory path

    def run(self):
        print("Welcome to MyDB CLI!")
        while True:
            user_input = input("MyDB > ").strip()

            # this corresponds to the inner loop break
            reset = False

            # Check for exit command
            if user_input.lower() == "exit()":
                print("Goodbye!")
                break

            # Split the user input into tokens
            tokens = user_input.split()

            # Check for valid command
            if len(tokens) < 2:
                print("Invalid command. Please follow the syntax.")
                continue

            if len(tokens) > 2:
                command = tokens[0]
                table_name = tokens[2]
                rest_of_command = ' '.join(tokens[3:])
            else:
                command = tokens[0]

            # handle CRUD cases
            if tokens[0] in CRUDSet:
                # handle different cases of commands
                if command == "create" and tokens[1] == "table":
                    # Example: create table person(a int, primary key a)
                    # only supports type str, int, float, bool
                    columns = rest_of_command.strip('(').strip(')').split(',')
                    dataType = []
                    primary_key = None
                    for i, c in enumerate(columns):
                        val = c.strip().split(" ")[0]
                        datatype = c.strip().split(" ")[1]
                        if val == "primary":
                            columns.remove(columns[i])
                        else:
                            columns[i] = val
                            dataType.append(datatype)

                        if c.strip().lower().startswith("primary"):
                            pVal = c.strip().split(" ")[-1]
                            if pVal not in columns:
                                print("Primary key specified is not an attribute in this table!")
                                reset = True
                                break
                            else:
                                primary_key = pVal
                    if not reset:
                        self.mydb.create_table(table_name, columns, primary_key, dataType)
                        self.mydb.save_table(self.directory)
                        self.mydb.save_metadata(self.directory)

                elif command == "insert" and tokens[1] == "table":
                    # Example: insert table person {name:"kevin", age:40}
                    table_name = tokens[2]
                    if table_name in self.mydb.table_names:
                        self.mydb.load_table(table_name, self.directory)
                        data = rest_of_command.strip('{').strip('}').split(',')
                        for i, d in enumerate(data):
                            col = d.strip().split(":")[0]
                            val = d.strip().split(":")[1]
                            if type(val) is str and val[0] != '"' and val[-1] != '"':
                                if "." in val:
                                    val = float(val)
                                elif val == "True" or val == "False":
                                    val = bool(val)
                                else:
                                    val = int(val)
                            elif type(val) is str:
                                val = val.replace('"', '')
                                val = val.strip(" ")

                            if "type" in self.mydb.current_table[table_name].keys():
                                if type(val) is not type_mapping.get(self.mydb.current_table[table_name]['type'][i], str):
                                    print("Type error: the insertion type does not match the table attribute's type!")
                                    reset = True
                                    break

                            if col != self.mydb.current_table[table_name]['columns'][i]:
                                print("Input Attribute error: the insertion attribute does not match the table attribute")
                                reset = True
                                break
                            data[i] = val
                        if not reset:
                            self.mydb.insert_into(table_name, data, self.directory)
                    else:
                        print("No table found")
                elif command == "update":
                    # update ON person SET age = 20 IF age < 20
                    #                       values   conditions
                    table_name = tokens[2]
                    index = 3
                    value_commands = ' '.join(tokens[index + 1: tokens.index("IF")])
                    rest_of_command = ' '.join(tokens[tokens.index("IF") + 1:])
                    # handel value_command
                    values = value_commands.split(" ")
                    # It will start FROM "IF" and be seperated by "AND" or the end of the list
                    if "OR" in rest_of_command:
                        condition_type = "OR"
                        conditions = rest_of_command.split("OR")
                    else:
                        condition_type = "AND"
                        conditions = rest_of_command.split("AND")
                    # this for loop will take the condition and separate each word into tokens
                    # every condition will be in the form like the examples ["Attribute" "operator" "values"]
                    # Ex: ['Age', '>', '25'], ['name', '=', '"John"']
                    for i, condition in enumerate(conditions):
                        c = condition.split()
                        conditions[i] = c
                    self.mydb.update_records(values, conditions, condition_type, table_name, self.directory)
                elif command == "delete":
                    # delete ON person IF age > 30
                    table_name = tokens[2]
                    index = 3
                    rest_of_command = ' '.join(tokens[index + 1:])
                    # find conditions
                    # It will start FROM "IF" and be seperated by "AND" or the end of the list
                    if "OR" in rest_of_command:
                        condition_type = "OR"
                        conditions = rest_of_command.split("OR")
                    else:
                        condition_type = "AND"
                        conditions = rest_of_command.split("AND")
                    # this for loop will take the condition and separate each word into tokens
                    # every condition will be in the form like the examples ["Attribute" "operator" "values"]
                    # Ex: ['Age', '>', '25'], ['name', '=', '"John"']
                    for i, condition in enumerate(conditions):
                        c = condition.split()
                        conditions[i] = c
                    # pass everything we handled into the functions
                    self.mydb.delete_records(conditions, condition_type, table_name, self.directory)
                    # pass
                else:
                    print("Unsupported command. Please check your syntax.")

            elif tokens[0] in SystemSet:
                if command == "load":
                    # Example: load csv from 'path_to_file.csv'
                    # EX: load example '/Users/Aven/Desktop/myDB/External/Employee.csv
                    file_path = tokens[2].strip("'")
                    table_name = tokens[1]
                    self.file_handler.load_csv(self.directory, file_path, table_name)
                    self.mydb.table_names.append(table_name)
                    self.mydb.save_metadata(self.directory)

                    # You can process the loaded data here
                    print(f"Loaded data from '{file_path}'.")

                elif command == "show" and tokens[1] == "table":
                    # Example: show table person
                    table_name = tokens[2]
                    self.mydb.load_table(table_name, self.directory)
                    self.mydb.show_table(table_name)

                elif command == "show" and tokens[1] == "tables":
                    # Example: show tables
                    self.mydb.load_metadata(self.directory)
                    self.mydb.show_tables()

                elif command == "drop" and tokens[1] == "table":
                    # drop table person
                    table_name = tokens[2]
                    self.mydb.drop_table(table_name, self.directory)
                    self.mydb.save_metadata(self.directory)

            else:
                # find conditions
                # It will start FROM "IF" and be seperated by "AND" or the end of the list
                if command == "FIND":
                    output_col_name = tokens[tokens.index("FIND") + 1:tokens.index("ON")]
                    table_name = tokens[tokens.index("ON") + 1]
                    joins, order_type, order_attribute, aggregate_type, aggregate_order_type, sum_type \
                        = [], "", "", "", "", ""
                    conditions_str = ""

                    if "IF" in tokens:
                        end_index = tokens.index("JOIN") if "JOIN" in tokens else tokens.index(
                            "in") if "ORDER" in tokens else len(tokens)
                        conditions_str = ' '.join(tokens[tokens.index("IF") + 1:end_index])

                    if "JOIN" in tokens:
                        end_index = tokens.index("ORDER") if "ORDER" in tokens else len(tokens)
                        joins = tokens[
                                tokens.index("JOIN") + 1: end_index]

                    if "ORDER" in tokens and tokens.index("ORDER") != len(tokens) - 1:
                        # FIND * ON shop IF age > 60 AND price > 1000 JOIN shopping BY customer_id EQUAL customer_id in descending ORDER OF age
                        order_type = tokens[tokens.index("ORDER") - 1] if tokens.index("ORDER") != len(
                            tokens) - 1 else ""
                        order_attribute = tokens[tokens.index("ORDER") + 2] if tokens.index("ORDER") != len(
                            tokens) - 1 else ""

                    if "COUNT" in tokens or "SUM" in tokens:
                        aggregate_type = "COUNT"
                        group_by = tokens[tokens.index("OF") + 1]
                        table_name = tokens[tokens.index("ON") + 1]
                        if "SUM" in tokens:
                            aggregate_type = "SUM"
                            sum_type = tokens[tokens.index("SUM") - 1]
                        if "ORDER" in tokens:
                            aggregate_order_type = tokens[tokens.index("ORDER") - 1]

                    if "OR" in conditions_str:
                        condition_type = "OR"
                        conditions = conditions_str.split("OR")

                    else:
                        condition_type = "AND"
                        conditions = conditions_str.split("AND")
                    # this for loop will take the condition and separate each word into tokens
                    # every condition will be in the form like the examples ["Attribute" "operator" "values"]
                    # Ex: ['Age', '>', '25'], ['name', '=', '"John"']
                    for i, condition in enumerate(conditions):
                        c = condition.split()
                        conditions[i] = c

                    # pass everything we handled into the functions
                    self.mydb.load_metadata(self.directory)
                    join_table, join_attribute1, join_attribute2 = "", "", ""
                    order_attribute_index = None
                    if joins:
                        join_table = joins[0]
                        join_attribute1 = joins[2]
                        join_attribute2 = joins[4]

                        if order_type:
                            self.mydb.load_table(table_name, self.directory)
                            order_attribute_index = self.mydb.current_table[table_name]['columns'].index(
                                order_attribute)
                            join.join_data(self.directory, table_name, join_table, join_attribute1, join_attribute2,
                                           conditions, condition_type, order_type, order_attribute,
                                           order_attribute_index, output_col_name)
                        elif aggregate_type:
                            aggregation(self.directory, table_name, aggregate_type, group_by, joins,
                                        aggregate_order_type, condition_type, conditions, sum_type)

                        else:
                            join.join_data(self.directory, table_name, join_table, join_attribute1, join_attribute2,
                                           conditions, condition_type, order_type, order_attribute,
                                           order_attribute_index, output_col_name)
                    else:
                        if order_type:
                            self.mydb.load_table(table_name, self.directory)
                            order_attribute_index = self.mydb.current_table[table_name]['columns'].index(
                                order_attribute)

                            current_directory = os.getcwd()
                            path = os.path.join(current_directory, "myDB", "Database", self.directory, "tables",
                                                table_name)
                            # path = f"/Users/Aven/Desktop/myDB/Database/{self.directory}/tables/{table_name}"
                            external_sort_and_write(path, table_name, self.directory, order_type, order_attribute,
                                                    order_attribute_index)
                            self.mydb.find_records(output_col_name, conditions, condition_type, table_name,
                                                   self.directory, order_type, order_attribute)
                        elif aggregate_type:
                            aggregation(self.directory, table_name, aggregate_type, group_by, joins,
                                        aggregate_order_type, condition_type, conditions, sum_type)
                        else:
                            self.mydb.find_records(output_col_name, conditions, condition_type, table_name,
                                                   self.directory, order_type, order_attribute)


