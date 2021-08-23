#!/usr/bin/env python

import json
import yaml
import pprint
from mo_sql_parsing import parse


def table_name_fmt(name):
    return name.replace('-', '_')


deps_dict = {}
commands_dict = {}

with open("example.yml", 'r') as stream:
    steps_dict = yaml.safe_load(stream)['steps']

    for step in steps_dict:
        if step.get('step_type') == 'sql-command':
            deps_dict[table_name_fmt(step.get('name'))] = step.get('depends_on')
            commands_dict[table_name_fmt(step.get('name'))] = step.get('command')

            parse(step.get('command'))

            # try this one instead
            # https://github.com/andialbrecht/sqlparse

# pprint.pprint(deps_dict)
# pprint.pprint(commands_dict)
