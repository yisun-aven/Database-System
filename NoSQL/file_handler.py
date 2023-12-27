# file_handler.py
import os
import json
class FileHandler:
    def __init__(self):
        # Initialize file-related attributes and configurations here
        pass

    def load_csv(self, directory, file_path, table_name, chunk_size=1000):
        # Implement the logic to load data from CSV files in chunks
        offset = 0
        count = 1
        with open(file_path, 'r', encoding="utf-8") as file:
            current_chunk = []
            for line in file:
                row = line.strip().split(',')
                current_chunk.append(row)
                if offset == 0:
                    header = current_chunk[0]

                # Check if the current chunk size exceeds the specified chunk_size
                if len(current_chunk) >= chunk_size:
                    self.save_csv(table_name, current_chunk, offset, directory, count, header)
                    offset += len(current_chunk)
                    count += 1
                    current_chunk.clear()
        if current_chunk:
            self.save_csv(table_name, current_chunk, offset, directory, count, header)
            offset += len(current_chunk)
            count += 1
            current_chunk.clear()

    def save_csv(self, table_name, data, offset, directory, count, header):
        # path = f"/Users/Aven/Desktop/myDB_NoSQL/Database/{directory}/tables/{table_name}"
        current_directory = os.getcwd()
        path = os.path.join(current_directory, "myDB", "Database", directory, "tables", table_name)

        if not os.path.exists(path):
            os.mkdir(path)
        json_header = json.dumps(header)
        if offset == 0:
            data = data[len(json_header):]
        json_data = json.dumps(data)

        res_json_data = "{" + f'"{table_name}":' + "{" + '"columns":' + json_header + ', "data":' + json_data[:-1] + "]}}"
        full_path = os.path.join(path, table_name + str(count) + ".json")
        with open(full_path, 'w') as file:
            file.write(res_json_data)
