import json
import pprint


def pp(json_dict):
    return print(json.dumps(json_dict, indent=4, sort_keys=True))


def table_name_fmt(name):
    return name.replace('-', '_')


def log(var_name, var):
    return print('{}{}{}'.format(str(var_name), ': ', var))
