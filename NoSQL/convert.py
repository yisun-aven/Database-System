import ijson
import csv


def flatten(prefix, item):
    """
    Flatten a JSON object into a single dictionary.
    """
    if isinstance(item, dict):
        for k, v in item.items():
            yield from flatten(f"{prefix}_{k}" if prefix else k, v)
    elif isinstance(item, list):
        for i, v in enumerate(item):
            yield from flatten(f"{prefix}_{i}" if prefix else str(i), v)
    else:
        yield (prefix, item)


def json_to_csv(json_file_path, csv_file_path):
    """
    Convert a JSON file to a CSV file.
    """
    with open(json_file_path, 'rb') as json_file:
        objects = ijson.items(json_file, 'item')
        flattened_data = []

        for obj in objects:
            flattened_data.append(dict(flatten('', obj)))

    keys = set().union(*(d.keys() for d in flattened_data))

    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=keys)
        writer.writeheader()
        writer.writerows(flattened_data)

    return csv_file_path
