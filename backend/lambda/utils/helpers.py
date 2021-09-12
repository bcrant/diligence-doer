import json
import os
import zipfile
from collections import defaultdict
from pathlib import Path


def log(var_name, var):
    return print('{:32s} {:6s} \t {}'.format(str(var_name), str(type(var)), str(var)))


def pp(json_dict):
    return print(json.dumps(json_dict, indent=4, sort_keys=True))


def table_name_fmt(name):
    return name.replace('-', '_')


def strip_brackets(field_name):
    return field_name.replace('[', '').replace(']', '')


def denormalize_json(nested_dict):
    flat_dict = {}
    nested_dicts = {}
    for key in nested_dict.keys():
        if type(nested_dict.get(key)) != dict:
            flat_dict[key] = nested_dict.get(key)
        else:
            for nested_key in nested_dict.get(key).keys():
                nested_dicts[key + '.' + nested_key] = nested_dict.get(key).get(nested_key)

    return {**nested_dicts, **flat_dict}


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


def unzip_packaged_tableau_file(zipped_file, output_file_type, output_dir, obj_name=None):
    """
    Tableau Packaged Workbooks (.twbx) and Packaged Datasources (.tdsx) are zip archives.
    Here we extract the given Workbook (.twb) or Datasource (.tds) only and return a path to the unzipped file.

    Args:
        zipped_file      (str): A path generated from the Download method of the Tableau Python Server Client Library
        output_file_type (str): The desired file extension ('twb', 'tds')
        output_dir       (str): Name of directory to store unzipped file ('tmp', 'tableau_files')
        obj_name         (str): The name of object downloaded from a Tableau Server
    Returns:
        unzipped_path    (str): The path to the unzipped file

    """

    with zipfile.ZipFile(zipped_file) as packaged_file:
        for f in packaged_file.namelist():
            if str('.' + output_file_type) in f:
                unzipped_path = packaged_file.extract(f, str('./' + output_dir))
                print(f'Extracting {f} file from {obj_name}...')
    return unzipped_path


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
