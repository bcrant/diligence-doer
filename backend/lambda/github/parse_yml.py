import io
import pprint
import yaml
from utils.helpers import *


def parse_yml(all_yml_files_list):
    print('Parsing YML files in repository...')

    #
    # Clean YAML file before loading
    #
    # Get environment variable key value pairs
    env_var_file_path = 'pipelines/example_template_config.yml'
    env_file = get_env_vars_dict(all_yml_files_list, env_var_file_path)

    # TODO: Add step to eliminate YML files with no Dataduct Steps
    # assert_sql_in_yml(all_yml_files_list)
    x = read_bytestream_to_yml(all_yml_files_list, env_file)
    print(x)

    # replace_yml_env_vars(file, env_dict)
    #
    # # Parse YML for each table's SQL/DDL/DML
    # commands_dict = {}
    # with open(file, 'r') as stream:
    #     steps_dict = yaml.safe_load(stream)['steps']


def read_bytestream_to_yml(yml_files_list, replace_dict=None):

    print('{:10s} {} {:10s}'.format('Found', len(yml_files_list), '.yml files'))

    problem_files_list = list()
    parsed_files_list = list()

    for yml_file in yml_files_list:
        print('\n\n')
        try:
            # print('{} {:64}'.format('Loading object to pyYaml for parsing... ', yml_file.path))
            lint_yml = lint_yml_for_parser(yml_file.decoded_content)
            print('linted...', type(lint_yml))

            clean_yml = replace_yml_env_vars(lint_yml, replace_dict)
            print('cleaned...', type(clean_yml))

            # print('{} {:64}'.format('Loading object to pyYaml for parsing... ', yml_file.path))
            read_yml_file = yaml.safe_load(
                stream=clean_yml
            )
            print('parsed into yml...')
            pp(list(read_yml_file.keys()))

            pop_layer = denormalize_json(read_yml_file)
            print('denormalized into yml...')
            pp(list(pop_layer.keys()))

            # log('read_yml_file', read_yml_file)

            # if 'steps' not in read_yml_file.keys():
            #     pass
            # else:
            # print('{} {:64}'.format('Object safely loaded file to pyYaml... ', yml_file.path))
            parsed_files_list.append(read_yml_file)

        except yaml.MarkedYAMLError as exc:
            print("Error while parsing YAML file: {:64}".format(yml_file.path))
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

    print('Successfully parsed {} .yml files.'.format((len(yml_files_list) - len(problem_files_list))))

    if len(problem_files_list) >= 1:
        print('Unable to parse the following files: ')
        for problem_file in problem_files_list:
            print('{:64} {}'.format(problem_file.name, problem_file.path))

    # return parsed_files_list
    return


def get_env_vars_dict(repo_files, env_file_path):
    print('Getting Github object for given repo file containing environment variables...')

    # Get environment variable key value pairs
    env_file = [f for f in repo_files if f.path == env_file_path]

    if env_file:
        print('Found match: {:32}'.format(env_file[0].html_url))

        # Loading pipeline config .yml file...
        lint_env_yml = lint_yml_for_parser(env_file[0].decoded_content)

        schema_names = yaml.safe_load(
            stream=lint_env_yml
        )

        flattened_schema_names = denormalize_json(denormalize_json(schema_names))

        return flattened_schema_names

    else:
        print('No match found or no file given: {}'.format(str(env_file_path)))

        return {'env_vars': 'none'}


def denormalize_json(nested_dict):
    flat_dict = {}
    nested_dicts = {}
    if nested_dict:
        for key in nested_dict.keys():
            if type(nested_dict.get(key)) != dict:
                flat_dict[key] = nested_dict.get(key)
            else:
                for nested_key in nested_dict.get(key).keys():
                    nested_dicts[key + '.' + nested_key] = nested_dict.get(key).get(nested_key)

    return {**nested_dicts, **flat_dict}


def lint_yml_for_parser(yml_bytestream):
    linted_bytes = io.BytesIO()
    lines = io.BytesIO(yml_bytestream).readlines()
    for line in lines:
        # Remove lines with breaking yml (%)
        if b'%' not in line:
            linted_bytes.write(line)

    return linted_bytes.getvalue()


def replace_yml_env_vars(yml_file, schema_names_dict):
    cleaned_bytes = io.BytesIO()

    lines = io.BytesIO(yml_file).readlines()
    for line in lines:
        # Find and replace schema environment variables with schema names
        if b'{{' not in line:
            cleaned_bytes.write(line)
        else:
            log('line', line)
            line_str = line.decode('UTF-8')
            log('line_str', line_str)
            env_var = line_str\
                .split('{{')[1]\
                .rsplit('}}')[0]
            if env_var.strip() in schema_names_dict.keys():
                replaced_line = line_str.replace(
                    str('{{' + env_var + '}}'),
                    str(schema_names_dict.get(env_var.strip()))
                )
                log('replaced_line', replaced_line)
                cleaned_bytes.write(replaced_line)
            else:
                print('ELSE...')
                log('line', line)
                log('line', str(line))
                stripped_line = line\
                    .replace(b'}}', b'')\
                    .replace(b'{{', b'')\
                    .strip(b' ')
                log('stripped_line', stripped_line)
                cleaned_bytes.write(stripped_line)

    return cleaned_bytes.getvalue()
