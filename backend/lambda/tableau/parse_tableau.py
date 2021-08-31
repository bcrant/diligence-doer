import os
import itertools
import zipfile
from pathlib import Path
import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
from sql_metadata import Parser
import xml.etree.ElementTree as ET
from utils.authentication import authenticate_tableau
from utils.dynamodb import write_to_dynamodb
from utils.helpers import log, pp, strip_brackets


def parse_tableau():
    # Tableau Authentication
    AUTHENTICATION, SERVER = authenticate_tableau()

    # Initialize dictionaries to store outputs
    metadata_dict = {}
    parsed_data_source_dict = {}

    # Log in to Tableau Server and query all Data Sources on Server
    with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):
        all_data_sources, pagination_item = SERVER.datasources.get()
        print('There are {} data sources on site...'.format(pagination_item.total_available))

        # Get initial metadata about all data sources on Tableau Server
        for datasource in all_data_sources[30:60]:
            # Filter out text files
            text_filters = ('hyper', 'webdata-direct', 'textscan')
            if datasource.datasource_type not in text_filters:
                # print(datasource.id, '\t', datasource.datasource_type, '\t', datasource.name)
                metadata_dict[datasource.id] = {
                    'datasource_id': datasource.id,
                    'datasource_name': datasource.name,
                    'datasource_type': datasource.datasource_type,
                    'connections': []
                }

        # Download only data sources that are not text files on Tableau Server and return a dict of paths to those files
        data_source_files = download_data_sources(SERVER, metadata_dict.keys())

        # Parse each data source file for its Custom SQL and store in dictionary
        for ds_id, ds_path in data_source_files.items():
            with open(ds_path) as f:
                tree = ET.parse(f)
            root = tree.getroot()

            # Get human readable name of datasource
            ds_name = metadata_dict[ds_id].get('datasource_name')

            # METADATA - Connection properties of data source xml
            connections = [c for c in root.iter('connection')]
            metadata = get_connection_metadata(connections)
            initial_sql = get_initial_sql(connections)
            parsed_initial_sql = parse_initial_sql(initial_sql)

            # SCHEMA - Relation properties of data source xml
            #
            # NOTE: It is important to collect the tables and columns from the
            # XML elements as well as the inner SQL text of Tableau data sources.
            #
            # Not all tables are defined in Custom or Initial SQL.
            #
            # Once connected to a database in Tableau, users can drag and drop
            # tables onto the data pane (where the Tableau Data Model is defined in UI).
            #
            relations = [r for r in root.iter('relation')]

            # These are the table and field names as defined in Custom SQL.
            custom_sql = get_custom_sql(relations)
            parsed_custom_sql = parse_custom_sql(custom_sql)

            # These are the table and field names as defined in the Tableau Data Model.
            tables = get_tables_from_xml(relations)
            columns = get_columns_from_xml(root)

            # TODO: possibly combine the custom sql and tableau data model
            #       table and field names here. Grab code from style validator
            #       for unpack unique

            # Update each output dictionaries with their respective parsed information
            metadata_dict[ds_name] = {
                'datasource_id': ds_id,
                **metadata,
                'raw_custom_sql': custom_sql,
                'raw_initial_sql': initial_sql,
                'parsed_custom_sql': parsed_custom_sql,
                'parsed_initial_sql': parsed_initial_sql
            }

            parsed_data_source_dict[ds_name] = {
                'datasource_id': ds_id,
                'tables': tables,
                'columns': columns,
                'source_tables': parsed_custom_sql.get('source_tables'),
                'source_table_aliases': parsed_custom_sql.get('source_table_aliases'),
                'field_names': parsed_custom_sql.get('field_names'),
                'field_name_aliases': parsed_custom_sql.get('field_names_aliases')
            }

            # print('xml_tables', type(tables), tables)
            # print('sql_tables', type(parsed_custom_sql.get('source_tables')), parsed_custom_sql.get('source_tables'))

    # print('METADATA DICT...')
    # pp(metadata_dict)

    # print('PARSED DATA SOURCE DICT...')
    # pp(parsed_data_source_dict)

    # # Remove all data source xml files from temporary directory
    # delete_tmp_files_of_type('.xml')


def download_data_sources(tableau_server, data_source_ids_list):
    # Store path to each file
    file_path_dict = {}

    # Download each data source on Tableau Server and parse for its Custom SQL and additional Metadata
    print('Extracting .tds files from each .tdsx')
    for ds_id in data_source_ids_list:
        zipped_ds_path = tableau_server.datasources.download(
            ds_id,
            filepath='./tmp',
            include_extract=False
        )

        # Packaged Datasource (.tdsx) files are zipped archives. Here we extract the Datasource (.tds) only.
        with zipfile.ZipFile(zipped_ds_path) as packaged_data_source:
            for ds_file in packaged_data_source.namelist():
                if '.tds' in ds_file:
                    unzipped_ds_path = packaged_data_source.extract(ds_file, './tmp')
                    # print(f'Extracting .tds file from...\t {ds_file}')

                    # Convert .tds to .xml
                    tds_file = Path(os.getcwd() + '/' + unzipped_ds_path)
                    tds_as_xml = tds_file.rename(tds_file.with_suffix('.xml'))

                    # Add path to dict
                    file_path_dict[ds_id] = tds_as_xml

                else:
                    print(f'No .tds file found in...\t\t {ds_file}')

    # Remove all .tdsx from temporary directory
    delete_tmp_files_of_type('.tdsx')

    return file_path_dict


def get_connection_metadata(connection_xml):
    connection_metadata = {}
    connection_types_list = []
    for conn in connection_xml:
        connection_type = conn.attrib.get('class')
        if connection_type != 'federated':
            connection_types_list.append(connection_type)

        # If connected to a cloud database, store additional connection_metadata
        if connection_type in ['snowflake', 'redshift']:
            connection_metadata['server_address'] = conn.attrib.get('server')
            connection_metadata['server_port'] = conn.attrib.get('port')
            connection_metadata['connection_dbname'] = conn.attrib.get('dbname')
            connection_metadata['connection_schema'] = conn.attrib.get('schema')
            connection_metadata['connection_warehouse'] = conn.attrib.get('warehouse')
            if bool(conn.attrib.get('one-time-sql')):
                connection_metadata['has_initial_sql'] = True

    connection_metadata['connections'] = connection_types_list

    return connection_metadata


def get_tables_from_xml(tables_xml):
    # Get table names
    tables_list = []
    for relation in tables_xml:
        if relation.attrib.get('type') == 'table':
            cleaned_table_name = strip_brackets(
                relation.attrib.get('table')
            )
            tables_list.append(cleaned_table_name)

    # log('tables_list', tables_list)
    return tables_list


def get_columns_from_xml(root_xml):
    columns_dict = {}

    # Get columns defined as map local-remote columns as key-value pairs
    cols_xml = root_xml.iter('cols')
    for c in cols_xml:
        for m in c.findall('map'):
            name = strip_brackets(m.attrib.get('key'))
            remote = strip_brackets(m.attrib.get('value'))

            columns_dict[name] = {
                'remote_column': remote
            }

    # Get Column names, aliases, and calculations
    columns_xml = [c for c in root_xml.iter('column')]
    for col in columns_xml:
        name = strip_brackets(
            col.attrib.get('name')
        )
        alias = col.attrib.get('caption')
        datatype = col.attrib.get('datatype')
        formula = None
        for calc in col.iter('calculation'):
            formula = calc.attrib.get('formula')

        columns_dict[name] = {}
        if alias:
            columns_dict[name]['alias'] = alias

        if datatype:
            columns_dict[name]['datatype'] = datatype

        if formula:
            columns_dict[name]['calculation'] = formula

    # Remove auto generated "Number of Records" field from dictionary
    columns_dict.pop('Number of Records')

    # log('columns_dict', columns_dict)
    return columns_dict


def get_initial_sql(initial_sql_xml):
    initial_sql_list = []
    for conn in initial_sql_xml:
        # Store Initial SQL query
        if bool(conn.attrib.get('one-time-sql')):
            # Format Custom SQL
            formatted_sql = sqlparse.format(
                conn.attrib.get('one-time-sql'),
                reindent=True,
                strip_comments=True,
                keyword_case='upper',
                comma_first=True
            )
            initial_sql_list.append(formatted_sql)

    return initial_sql_list


def get_custom_sql(custom_sql_xml):
    # Get Custom SQL
    custom_sql_list = []
    for sql_item in custom_sql_xml:
        if sql_item.attrib.get('type') == 'text':
            # Format Custom SQL
            formatted_sql = sqlparse.format(
                sql_item.text,
                reindent=True,
                strip_comments=True,
                keyword_case='upper',
                comma_first=True
            )
            custom_sql_list.append(formatted_sql)

    return custom_sql_list


def parse_custom_sql(custom_sql_list):
    # Parse SQL
    table_metadata_dict = {}
    for q in custom_sql_list:
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

        except ValueError:
            print('Attempting to handle value error...')
            continue

        except KeyError:
            continue

        #
        # Store Output
        #
        table_metadata_dict = {
            'source_tables': table_names,
            'source_table_aliases': table_aliases_dict,
            'field_names': columns,
            'field_name_aliases': column_aliases_dict
        }

    return table_metadata_dict


def handle_unsupported_query_types(unsupported_sql):
    parsed = sqlparse.parse(unsupported_sql)
    grouped = sqlparse.parse().tokens.group
    # # log('parsed', parsed)
    # for line in unsupported_sql.splitlines():
    #     print('...', line)

    # return parsed

    # if 'SELECT' in unsupported_sql:

    # if 'SELECT' in unsupported_sql:
    #     sep = 'SELECT'
    #     log('unsupported_sql', unsupported_sql)
    #     splitted = unsupported_sql.split(sep, maxsplit=1)[1]
    #     stmt = f'{sep}{splitted}'
    #     split_stmt = sqlparse.split(stmt)[0]
    #     log('split_stmt', split_stmt)
    #
    #     return split_stmt
    #
    # else:
    #     print('No remote database referenced in Initial SQL.')


def parse_initial_sql(initial_sql_list):
    # Parse SQL
    table_metadata_dict = {}
    for get_q in initial_sql_list:
        f_token = sqlparse.parse(get_q)[0].token_first()
        if not f_token.match(Keyword.DML, 'SELECT'):
            q = handle_unsupported_query_types(get_q)
        else:
            q = get_q

        if q:
            try:
                #
                # Table Names
                #
                log('q', q)
                table_names = [t for t in Parser(q).tables]
                log('table_names', table_names)

                table_aliases_dict = Parser(q).tables_aliases
                # log('table_aliases_dict', table_aliases_dict)

                #
                # Column Names
                #
                columns = [c for c in Parser(q).columns]
                # log('columns', columns)

                column_aliases_dict = Parser(q).columns_aliases
                # log('column_aliases_dict', column_aliases_dict)

            except ValueError:
                continue

            except KeyError:
                continue

            #
            # Store Output
            #
            table_metadata_dict = {
                'source_tables': table_names,
                'source_table_aliases': table_aliases_dict,
                'field_names': columns,
                'field_name_aliases': column_aliases_dict
            }

    return table_metadata_dict


def delete_tmp_files_of_type(filetype_str):
    tmp_dir = os.getcwd() + '/tmp'
    for f in os.listdir(Path(tmp_dir)):
        if filetype_str in f:
            os.remove(Path(tmp_dir + '/' + f))


if __name__ == "__main__":
    parse_tableau()
