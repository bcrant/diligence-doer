import json
import os
from pathlib import Path


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


def convert_tableau_file_to_xml(file_path):
    """
    Converts a given Tableau file to XML

    Args:
        file_path (str): The path to the file to convert
    Returns:
        converted_file_path (str): The path to the converted file

    """
    in_file_path = Path(os.getcwd() + '/' + file_path)
    converted_file_path = in_file_path.rename(in_file_path.with_suffix('.xml'))
    return converted_file_path


def delete_tmp_files_of_type(filetype_str, dir_name):
    """
    Delete files of a given extension type and directory from file system

    Args:
        filetype_str (str): The file extension of the file(s) to be removed: ex. 'html'
        dir_name (str): The directory containing the file(s) to be removed: ex. 'tmp'
    Return:
        None
    """
    tmp_dir = os.getcwd() + '/' + dir_name
    for f in os.listdir(Path(tmp_dir)):
        if str('.' + filetype_str) in f:
            os.remove(Path(tmp_dir + '/' + f))
        else:
            print(f'No files of type "{filetype_str}" found in "{tmp_dir}"')
