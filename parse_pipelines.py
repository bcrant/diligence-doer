#!/usr/bin/env python

import yaml
import pprint
import sqlparse
import sql_metadata
from sql_metadata import Parser, compat


def table_name_fmt(name):
    return name.replace('-', '_')


commands_dict = {}

with open("example.yml", 'r') as stream:
    steps_dict = yaml.safe_load(stream)['steps']

    for step in steps_dict:
        # if step.get('step_type') == 'sql-command':
        # isolate one table for testing only
        if step.get('name') == 'sponsored-learning-enrollment-activities':
            # FORMAT
            formatted_statement = sqlparse.format(
                step.get('command'),
                reindent=True,
                strip_comments=True,
                keyword_case='upper',
                comma_first=True
            )

            # SPLIT
            split_statement = sqlparse.split(formatted_statement)

            # STORE
            commands_dict[table_name_fmt(step.get('name'))] = split_statement

query = commands_dict.get('sponsored_learning_enrollment_activities')
for q in query:
    if str(sqlparse.parse(q)[0].token_first()) == 'INSERT':
        # print(type(q), q)
        tables = [t for t in Parser(q).tables]
        pprint.pprint(tables)

        columns = [c for c in Parser(q).columns]
        pprint.pprint(columns)

        # columns_dict = [cd for cd in Parser(q).columns]
        # pprint.pprint(columns_dict)

        alias_dict = {}
        for col in Parser(q).columns:
            if Parser(q).columns_aliases_dict is not None:



        print('\n\n')


    # try:
    #     sql_metadata.compat.get_query_tables(q)
    # except e:
    #     print(e)

    # # print(type(q), len(q), q)
    # parsed = sqlparse.parse(q)[0]
    # tokens = parsed.get_sublists()
    # print('q: ', q)
    # print('tokens: ', parsed.tokens)
    # print('# of tokens: ', len(parsed.tokens))
    # print('get_type: ', parsed.get_type())
    # print('get_sublists: ', next(parsed.get_sublists()))
    # print('\n\n')

# for phrase in query:
#     print(type(phrase), phrase)
#     # print(phrase.split('CREATE TABLE'))
#     # x = Parser(phrase).columns
#     # print(x)
