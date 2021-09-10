import io

import yaml
from io import BytesIO
from utils.helpers import *


def read_bytestream_to_yml(yml_files_list):

    print('{:10s} {} {:10s}'.format('Found', len(yml_files_list), '.yml files'))

    problem_files_list = list()

    for yml_file in yml_files_list[0:20]:

        try:
            print('{:32s} {}'.format('Loading object to pyYaml for parsing... ', yml_file.path))
            clean_yml = lint_yml_for_parser(yml_file.decoded_content)
            log('clean_yml', clean_yml)
            read_yml_file = yaml.safe_load(
                stream=clean_yml
            )

            print('{:32s} {}'.format('Object safely loaded file to pyYaml... ', yml_file.path))
            print(read_yml_file)


            # flattened_yml_file = denormalize_json(read_yml_file)
            # pp(list(flattened_yml_file.keys()))
            #
            # pp(list(read_yml_file.keys()))

        except yaml.MarkedYAMLError as exc:
            print("Error while parsing YAML file: {}".format(yml_file.path))
            problem_files_list.append(yml_file)

            if hasattr(exc, 'problem_mark'):
                if not exc.context:
                    print('  parser says\n', str(exc.problem_mark), '\n  ',
                          str(exc.problem), ' ', str(exc.context), '\nPlease correct data and retry.')
                else:
                    print('  parser says\n', str(exc.problem_mark), '\n  ',
                          str(exc.problem), '\nPlease correct data and retry.')
            else:
                print("Something went wrong while parsing yaml file")

    print('\n\n')
    print('Successfully parsed {} .yml files.'.format((len(yml_files_list) - len(problem_files_list))))
    print('Unable to parse the following files: ')
    for problem_file in problem_files_list:
        print('{:32} {}'.format(problem_file.name, problem_file.path))

    return


def denormalize_json(nested_dict):
    flat_dict = {}
    nested_dicts = {}
    if nested_dict:
        for key in nested_dict.keys():
            if type(nested_dict.get(key)) != dict:
                flat_dict[key] = nested_dict.get(key)
            else:
                for nested_key in nested_dict.get(key).keys():
                    nested_dicts[key + '#' + nested_key] = nested_dict.get(key).get(nested_key)

    return {**nested_dicts, **flat_dict}


def lint_yml_for_parser(yml_bytestream):
    cleaned_bytes = io.BytesIO()
    # with open(yml_bytestream, 'rb') as f_in:
    lines = io.BytesIO(yml_bytestream).readlines()
    for line in lines:
        # Remove lines with breaking yml (%)
        if b'%' not in line:
            cleaned_bytes.write(line)
        else:
            cleaned_bytes.write(line.replace(b'%', b''))

    print('\n\n')
    log('cleaned_bytes', cleaned_bytes)
    print(cleaned_bytes.getvalue())

    return cleaned_bytes.getvalue()
