import io
import sqlparse
import yaml
from sqlparse import exceptions
from sql_metadata import Parser
from utils.helpers import *


def parse_yml(all_yml_files_list):
    print('Parsing YML files in repository...')

    # Clean YAML file before loading
    env_var_file_path = 'pipelines/example_template_config.yml'
    env_file = get_env_vars_dict(all_yml_files_list, env_var_file_path)

    # Identify and parse YML files containing SQL
    parsed_yml_dict = read_bytestream_to_yml(all_yml_files_list, replace_dict=env_file)

    # Get unique table appearances in each source file
    table_to_repo_file_dict = invert_dict_key_values(
        get_unique_tables_per_file(parsed_yml_dict)
    )

    return table_to_repo_file_dict


def get_unique_tables_per_file(parsed_files):
    print('\n\n')
    print('Getting unique table references in file...')
    unique_tables_per_file = dict()

    for file, contents in parsed_files.items():
        unique_tables_list = list()
        for tables in list(v.get('source_tables') for k, v in parsed_files.get(file).items()):
            for table in tables:
                if '.' in table:
                    cleaned_table = table\
                        .replace('_tmp', '')\
                        .replace('_temp', '')\
                        .replace('_staging', '')

                    if cleaned_table not in unique_tables_list:
                        unique_tables_list.append(cleaned_table)

        unique_tables_per_file[file] = unique_tables_list

    # pp(unique_tables_per_file)

    return unique_tables_per_file


def read_bytestream_to_yml(yml_files_list, replace_dict=None):

    print('{:10s} {} {:10s}'.format('Found', len(yml_files_list), '.yml files'))

    problem_files_list = list()
    parsed_files_dict = dict()

    for yml_file in yml_files_list:
        try:
            #
            # Processing YML file
            #
            yml_bytestream = io.BytesIO(yml_file.decoded_content)

            if bool(verify_sql_steps_in_yml(yml_bytestream)):
                print()
                print('{:64s} {}'.format('Found SQL in YML file...', yml_file.path))
                print('{:64s} {}'.format('Preprocessing YML file for parsing...', yml_file.path))
                linted_yml_file = lint_yml_for_parser(yml_bytestream)
                cleaned_yml_file = replace_yml_env_vars(linted_yml_file, replace_dict)

                print('{:64s} {}'.format('Loading YML file for parsing... ', yml_file.path))
                read_yml_file = yaml.safe_load(
                    stream=cleaned_yml_file
                )

                #
                # Parsing SQL from YML files
                #
                print('{:64s} {}'.format('Done loading. Getting "steps" of type "sql-command"... ', yml_file.path))
                file_steps_dicts = dict()
                for step in read_yml_file.get('steps'):
                    if bool(step.get('command')) and step.get('step_type') == 'sql-command':
                        # Trim excess YML clauses before passing object
                        for key in list(step.keys()):
                            if key not in ('name', 'command'):
                                step.pop(key)

                        # Restructure steps into name-command key-value pairs and parse command for sql
                        file_steps_dicts[table_name_fmt(step.get('name'))] = parse_sql_from_command(step.get('command'))

                # Parse tables and fields from restructured steps
                parsed_output = parse_tables_and_fields_from_sql(file_steps_dicts)

                #
                # Output
                #
                parsed_files_dict[yml_file.html_url] = parsed_output

            else:
                print('{:64s} {}'.format('No "steps" for SQL found in YML file...', yml_file.path))

        except yaml.MarkedYAMLError as exc:
            #
            # Whole bunch of YML error marshaling for finicky parsing library
            #
            print("Error while parsing YAML file: {:64s}".format(yml_file.path))
            problem_files_list.append(yml_file)

            if hasattr(exc, 'problem_mark'):
                if not exc.context:
                    print('  parser says\n', str(exc.problem_mark), '\n  ',
                          str(exc.problem), ' ', str(exc.context), '\nPlease correct data and retry.')
                else:
                    print('  parser says\n', str(exc.problem_mark), '\n  ',
                          str(exc.problem), '\nPlease correct data and retry.')
            else:
                print("Something went wrong while parsing YML file")

    print('\n\n')
    print('Successfully parsed __{}__ YML files containing SQL.'.format(len(parsed_files_dict)))

    if len(problem_files_list) >= 1:
        print('Unable to parse the following files: ')
        for problem_file in problem_files_list:
            print('{:64} {}'.format(problem_file.name, problem_file.path))

    return parsed_files_dict


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


def verify_sql_steps_in_yml(repo_yml_file):
    top_match = b'steps:\n'
    inner_match = b'- step_type: sql-command\n'

    if top_match and inner_match in repo_yml_file.getvalue():
        split_yml = top_match + repo_yml_file.getvalue().split(top_match)[1]
        return split_yml
    else:
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
                if 'depends' not in env_var.strip():
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


def parse_sql_from_command(command):
    # Format SQL
    formatted_statement = sqlparse.format(
        command,
        reindent=True,
        strip_comments=True,
        keyword_case='upper',
        comma_first=True
    )

    # Split SQL by statement (a phrase ending in a semicolon)
    split_statement = sqlparse.split(formatted_statement)

    return split_statement


def parse_tables_and_fields_from_sql(commands_dict):
    # Parse each table's SQL/DDL/DML for table and column information
    table_metadata_dict = {}
    for table in sorted(commands_dict.keys()):
        print('Identifying source tables and field names for...', table.upper())

        for q in commands_dict.get(table):
            if str(sqlparse.parse(q)[0].token_first()) == 'INSERT':
                try:
                    #
                    # Table Names
                    #
                    table_names = [t for t in Parser(q).tables]
                    # log('table_names', table_names)

                    #
                    # Field Names
                    #
                    fields = [c for c in Parser(q).columns]
                    # log('columns', columns)

                    #
                    # Table and Field Aliases (excluding for now)
                    #
                    # table_aliases_dict = Parser(q).tables_aliases
                    # log('table_aliases_dict', table_aliases_dict)
                    #
                    # field_aliases_dict = Parser(q).columns_aliases
                    # log('column_aliases_dict', column_aliases_dict)

                    #
                    # Store Output
                    #
                    table_metadata_dict[table] = {
                        'source_tables': table_names,
                        'field_names': fields
                        # 'source_table_aliases': table_aliases_dict,
                        # 'field_name_aliases': field_aliases_dict
                    }

                except ValueError as v_err:
                    print(f'[SQL PARSE] ValueError: {v_err}')
                    continue

                except KeyError as k_err:
                    print(f'[SQL PARSE] KeyError: {k_err}')
                    continue

                except AttributeError as a_err:
                    print(f'[SQL PARSE] AttributeError: {a_err}')
                    continue

                except sqlparse.exceptions as sql_err:
                    print(f'[SQL PARSE] SQL PARSE Exception: {sql_err}')
                    continue

    return table_metadata_dict
