#!/usr/bin/env python

import yaml
import sqlparse
from sql_metadata import Parser
from helpers import table_name_fmt, log

commands_dict = {}

print('Loading pipeline .yml file...')
with open("example.yml", 'r') as stream:
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

table_metadata_dict = {}
for table in sorted(commands_dict.keys()):
    print('Identifying source tables and field names for...', table.upper())

    for q in commands_dict.get(table):
        if str(sqlparse.parse(q)[0].token_first()) == 'INSERT':
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

print(table_metadata_dict)
