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
    read_bytestream_to_yml(all_yml_files_list, replace_dict=env_file)

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
        try:
            # Get bytestream
            yml_bytestream = io.BytesIO(yml_file.decoded_content)

            if bool(verify_sql_steps_in_yml(yml_bytestream)):

                print('\n\n')
                print('{:64s} {}'.format('Preprocessing YML file... ', yml_file.path))

                linted_yml_file = lint_yml_for_parser(yml_bytestream)
                print('linted_yml_file', type(linted_yml_file), linted_yml_file[0:100])

                cleaned_yml_file = replace_yml_env_vars(linted_yml_file, replace_dict)
                print('cleaned_yml_file', type(cleaned_yml_file), cleaned_yml_file[0:100])

                # for line in io.BytesIO(cleaned_yml_file).readlines():
                #     print(line)

                print('{:64s} {}'.format('Loading YML file for parsing... ', yml_file.path))
                read_yml_file = yaml.safe_load(
                    stream=cleaned_yml_file
                )

                # print('parsed into yml...')
                # pp(list(read_yml_file.keys()))
                #
                # pop_layer = denormalize_json(read_yml_file)
                # print('denormalized into yml...')
                # pp(list(pop_layer.keys()))

                # log('read_yml_file', read_yml_file)

                print('{} {:64}'.format('Object safely loaded file to pyYaml... ', yml_file.path))
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

    print('Successfully parsed all {} .yml files.'.format(len(parsed_files_list)))

    if len(problem_files_list) >= 1:
        print('Unable to parse the following files: ')
        for problem_file in problem_files_list:
            print('{:64} {}'.format(problem_file.name, problem_file.path))


def get_env_vars_dict(repo_files, env_file_path):
    print('Getting Github object for given repo file containing environment variables...')

    # Get environment variable key value pairs
    env_file = [f for f in repo_files if f.path == env_file_path]

    if env_file:
        print('Found match: {:32}'.format(env_file[0].html_url))

        # Loading pipeline config .yml file...
        env_yml_bytestream = io.BytesIO(env_file[0].decoded_content)
        lint_env_yml = lint_yml_for_parser(env_yml_bytestream)

        schema_names = yaml.safe_load(
            stream=lint_env_yml
        )

        flattened_schema_names = denormalize_json(
            denormalize_json(
                {**schema_names.get('root'), **schema_names.get('default_schema_names')}
            )
        )

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


def verify_sql_steps_in_yml(repo_yml_file):
    top_match = b'steps:\n'
    inner_match = b'- step_type: sql-command\n'

    if top_match and inner_match in repo_yml_file.getvalue():
        print('Found SQL in YML file...')
        split_yml = top_match + repo_yml_file.getvalue().split(top_match)[1]
        return split_yml
    else:
        print('No Steps for SQL Commands found in YML file. Skipping...')
        return False


def lint_yml_for_parser(repo_yml_file_bytestream):

    linted_bytes = io.BytesIO()
    lines = repo_yml_file_bytestream.readlines()

    for line in lines[0:-3]:
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
            # log('no variables in line...', line)
            cleaned_bytes.write(line)
        else:
            # log('found variables in line...', line)
            line_str = line.decode(encoding='UTF-8')
            env_var = line_str\
                .split('{{')[1]\
                .rsplit('}}')[0]
            # log('env_var', env_var)
            if env_var.strip() not in schema_names_dict.keys():
                stripped_line = line_str\
                    .replace('}}', '')\
                    .replace('{{', '')\
                    .replace(env_var, str(env_var.replace(' ', '')))
                # log('stripped_line', stripped_line)
                cleaned_bytes.write(stripped_line.encode(encoding='UTF-8'))
            else:
                replaced_line = line_str.replace(
                    str('{{' + env_var + '}}'),
                    str(schema_names_dict.get(env_var.strip()))
                )
                # log('replaced_line', replaced_line)
                cleaned_bytes.write(replaced_line.encode(encoding='UTF-8'))

    return cleaned_bytes.getvalue()
