import io
import sqlparse
from sqlparse import exceptions
from sql_metadata import Parser
from utils.helpers import *


def parse_sql(sql_files_list, file_to_url_dict, env_replace_dict):
    print('{:10s} {} {:10s}'.format('Found', len(sql_files_list), '.sql files'))

    parsed_sql_dict = dict()

    # Identify and parse SQL files containing SQL
    for sql_file in sql_files_list:
        # try:
        print('{:64s} {}'.format('Preprocessing SQL file for parsing...', sql_file.path))
        #
        # Processing sql file
        #
        sql_bytestream = io.BytesIO(sql_file.decoded_content)

        cleaned_sql_file = replace_sql_env_vars(sql_bytestream.getvalue(), env_replace_dict)
        # print(cleaned_sql_file)

        split_sql_statements_list = parse_sql_from_command(cleaned_sql_file)

        parsed_sql_dict[sql_file.name] = split_sql_statements_list

    # Parse tables and fields from restructured steps
    parsed_tables_dict = parse_tables_and_fields_from_sql(parsed_sql_dict)

    # Get unique table appearances in each source file
    table_to_file_dict = map_file_to_url(
        get_unique_tables_per_sql_file(parsed_tables_dict),
        file_to_url_dict
    )

    pp(table_to_file_dict)

    return


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
        print('{:64s} {}'.format('Identifying source tables and field names for...', table.upper()))

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
                    # fields = [c for c in Parser(q).columns]
                    # log('columns', fields)

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
                        # 'field_names': fields
                        # 'source_table_aliases': table_aliases_dict,
                        # 'field_name_aliases': field_aliases_dict
                    }

                except ValueError:
                    # print('{:64s} {}'.format('[SQL PARSE] ValueError: ', v_err))
                    continue

                except KeyError as k_err:
                    print('{:64s} {}'.format('[SQL PARSE] KeyError: ', k_err))
                    continue

                except AttributeError as a_err:
                    print('{:64s} {}'.format('[SQL PARSE] AttributeError: ', a_err))
                    continue

                except sqlparse.exceptions as sql_err:
                    print('{:64s} {}'.format('[SQL PARSE] SQL PARSE Exception: ', sql_err))
                    continue

    return table_metadata_dict


def replace_sql_env_vars(sql_file, schema_names_dict):
    cleaned_bytes = io.BytesIO()

    lines = io.BytesIO(sql_file).readlines()
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


def get_unique_tables_per_sql_file(parsed_files):
    print('\n\n')
    print('Getting unique table references in file...')
    unique_tables_per_file = dict()

    for file, contents in parsed_files.items():
        unique_tables_list = list()
        for table in list(contents.get('source_tables')):
            if '.' in table:
                cleaned_table = table\
                    .replace('_tmp', '')\
                    .replace('_temp', '')\
                    .replace('_staging', '')

                if cleaned_table.lower() not in unique_tables_list:
                    unique_tables_list.append(cleaned_table.lower())

        unique_tables_per_file[file] = unique_tables_list

    return unique_tables_per_file
