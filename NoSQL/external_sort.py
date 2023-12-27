import heapq
import os
import json
import ijson


# FIND Continent Name GNP ON country in descdbending ORDER OF GNP
def load_sorted_block(file, table_name):
    return ijson.items(file, f'{table_name}.data.item')


def external_sort_and_write(path, table_name, directory_name, order_type, order_attribute, order_attribute_index):
    sorted_files = []
    # Get the list of files and directories in the specified path
    file_names = os.listdir(path)
    # Filter out only the files
    if not os.path.exists(os.path.join(path, f'sorted_{table_name}')):
        os.makedirs(os.path.join(path, f'sorted_{table_name}'))

    full_path = os.path.join(path, f'sorted_{table_name}')

    if not os.path.exists(os.path.join(full_path, f'{order_attribute}')):
        os.makedirs(os.path.join(full_path, f'{order_attribute}'))
    else:
        return

    attribute_path = os.path.join(full_path, f'{order_attribute}')
    # Step 1 & 2: Sort individual blocks and write them back to disk
    for file_name in file_names:
        if file_name.endswith(".json"):
            file_path = os.path.join(path, file_name)
            with open(file_path, 'r') as file:
                table = json.load(file)
                data = table[f'{table_name}']['data']
                data.sort(key=lambda x: float(x[order_attribute_index]))
                table[f'{table_name}']['data'] = data
                sorted_file_name = f"{file_name[:-5] + '_' + order_attribute}_sorted.json"
                sorted_file_path = os.path.join(attribute_path, sorted_file_name)
                with open(sorted_file_path, 'w') as sorted_file:
                    json.dump(table, sorted_file)
                sorted_files.append(sorted_file_name)

    # Step 3: Merge sorted blocks
    sorted_blocks = [load_sorted_block(open(os.path.join(attribute_path, file_name), 'r'), table_name) for file_name in sorted_files]
    sorted_all_data = heapq.merge(*sorted_blocks, key=lambda x: float(x[order_attribute_index]))
    dataSetSize = []
    for file_name in sorted(sorted_files):
        file_path = os.path.join(attribute_path, file_name)
        with open(file_path, 'r') as file:  # Open the file in read mode
            table = json.load(file)
        size = len(table[table_name]['data'])
        dataSetSize.append(size)

    # step 4: Write it back
    if dataSetSize[0] == 0:
        count = 1
    else:
        count = 0
    chunk = []
    for records in sorted_all_data:
        chunk.append(records)
        if len(chunk) == dataSetSize[count]:
            file_name = sorted(sorted_files)[count]
            file_path = os.path.join(attribute_path, file_name)
            with open(file_path, 'r') as file:  # Open the file in read mode
                table = json.load(file)
            table[table_name]['data'] = []
            table[table_name]['data'].extend(chunk)

            if os.path.isfile(file_path):
                # Delete the file
                os.remove(file_path)

            with open(file_path, 'w+') as file:  # Open the file in write mode
                json.dump(table, file)
            count += 1
            chunk = []



