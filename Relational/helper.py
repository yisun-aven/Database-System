# helper.py
# some helper functions
type_mapping = {
    'str': str,
    'int': int,
    'float': float,
    'bool': bool,
}

# Commands set
CRUDSet = ("create", "insert", "delete", "update")
SystemSet = ("show", "load", "drop")


def is_float(string):
    if string == "nan" or string == "NAN":
        return False
    try:
        float_value = float(string)
        return True
    except ValueError:
        return False


def compare(x, operator, y):
    if type(x) is int:
        y = int(y)
    elif type(x) is str:
        y = str(y)
    elif type(x) is bool:
        y = bool(y)
    elif type(x) is float:
        y = float(y)

    if operator == '=':
        return True if x == y else False
    elif operator == '>':
        return True if x > y else False
    elif operator == '>=':
        return True if x >= y else False
    elif operator == '<':
        return True if x < y else False
    elif operator == '<=':
        return True if x <= y else False
    elif operator == '!=':
        return True if x != y else False
    else:
        print("Invalid operator!")
    return
