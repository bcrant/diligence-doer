#!/usr/bin/env python

import yaml
import sqlparse
from sql_metadata import Parser
from utils.helpers import table_name_fmt, pp


def parse_pipelines():

    #
    # Clean YAML file before loading
    #
    file = 'pipeline_files/example.yml'
    lint_yml_for_parser(file)

    env_dict = get_schema_names_dict('pipeline_files/example_config.yml')
    replace_yml_env_vars(file, env_dict)

    # Parse YML for each table's SQL/DDL/DML
    commands_dict = {}
    with open(file, 'r') as stream:
        steps_dict = yaml.safe_load(stream)['steps']

        print('Parsing SQL commands from pipeline steps...')
        for step in steps_dict:
            if step.get('step_type') == 'sql-command':
                # Format SQL
                formatted_statement = sqlparse.format(
                    step.get('command'),
                    reindent=True,
                    strip_comments=True,
                    keyword_case='upper',
                    comma_first=True
                )

                # Split SQL by statement (a phrase ending in a semicolon)
                split_statement = sqlparse.split(formatted_statement)

                # Store as key, value pairs
                commands_dict[table_name_fmt(step.get('name'))] = split_statement

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

                    table_aliases_dict = Parser(q).tables_aliases
                    # log('table_aliases_dict', table_aliases_dict)

                    #
                    # Column Names
                    #
                    columns = [c for c in Parser(q).columns]
                    # log('columns', columns)

                    column_aliases_dict = Parser(q).columns_aliases
                    # log('column_aliases_dict', column_aliases_dict)

                    #
                    # Store Output
                    #
                    table_metadata_dict[table] = {
                        'source_tables': table_names,
                        'source_table_aliases': table_aliases_dict,
                        'field_names': columns,
                        'field_name_aliases': column_aliases_dict
                    }

                except ValueError:
                    continue

                except KeyError:
                    continue

    pp(table_metadata_dict)


def get_schema_names_dict(config_file):
    # Loading pipeline config .yml file...
    with open(config_file, 'r') as c:
        schema_names = yaml.safe_load(c)['root']

    flattened_schema_names = denormalize_json(denormalize_json(schema_names))

    return flattened_schema_names


def lint_yml_for_parser(file_path):
    with open(file_path, 'r') as f_in:
        lines = f_in.readlines()

    with open(file_path, 'w') as f_out:
        for line in lines[0:-3]:
            # Remove lines with breaking yml (%)
            if '%' not in line:
                f_out.write(line)


def replace_yml_env_vars(file_path, schema_names_dict):
    with open(file_path, 'r') as f_in:
        lines = f_in.readlines()

    with open(file_path, 'w') as f_out:
        for line in lines:
            # Find and replace schema environment variables with schema names
            if '{{' not in line:
                f_out.write(line)
            else:
                env_var = line\
                    .split('{{')[1]\
                    .rsplit('}}')[0]
                if env_var.strip() in schema_names_dict.keys():
                    replaced_line = line.replace(
                        '{{' + env_var + '}}',
                        str(schema_names_dict.get(env_var.strip()))
                    )
                    f_out.write(replaced_line)
                else:
                    stripped_line = line\
                        .replace('}}', '')\
                        .replace('{{', '')
                    f_out.write(stripped_line)


if __name__ == "__main__":
    parse_pipelines()
