import json
import pprint


def pp(json_dict):
    return print(json.dumps(json_dict, indent=4, sort_keys=True))


def table_name_fmt(name):
    return name.replace('-', '_')


def strip_brackets(field_name):
    return field_name.replace('[', '').replace(']', '')


def log(var_name, var):
    # if type(var) == str:
    #     var = var.replace('\n', '').strip()
    return print('{:32s} {:6s} \t {}'.format(str(var_name), str(type(var)), str(var)))
