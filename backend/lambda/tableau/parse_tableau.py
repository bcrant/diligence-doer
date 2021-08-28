import os
import zipfile
import sqlparse
from sql_metadata import Parser
import xml.etree.ElementTree as ET
import tableauserverclient as TSC
from pathlib import Path
from dotenv import load_dotenv
from helpers import pp


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
        for datasource in all_data_sources[10:20]:
            # Filter out text files
            text_filters = ('hyper', 'webdata-direct', 'textscan')
            if datasource.datasource_type not in text_filters:
                print(datasource.id, '\t', datasource.datasource_type, '\t', datasource.name)
                metadata_dict[datasource.id] = {
                    'datasource_id': datasource.id,
                    'datasource_name': datasource.name,
                    'datasource_type': datasource.datasource_type,
                    'connections': []
                }

        # Download each data source on Tableau Server and return a dict of paths to those files
        data_source_files = download_data_sources(SERVER, metadata_dict.keys())

        # Parse each data source file for its Custom SQL and store in dictionary
        for ds_id, ds_path in data_source_files.items():
            print(ds_id, ds_path)
            with open(ds_path) as f:
                tree = ET.parse(f)
            root = tree.getroot()

            # METADATA - Connection properties of data source xml
            connections = root.iter('connection')
            metadata = get_connection_metadata(connections)
            initial_sql = get_initial_sql(connections)
            parsed_initial_sql = parse_custom_sql(
                format_custom_sql(initial_sql)
            )

            # SCHEMA - Relation properties of data source xml
            relations = root.iter('relation')
            custom_sql = get_custom_sql(relations)
            parsed_custom_sql = parse_custom_sql(
                format_custom_sql(custom_sql)
            )

            tables = get_tables_from_xml(relations)
            columns = get_columns_from_xml(root.iter('column'))

            # Update each output dictionaries with their respective parsed information
            metadata_dict[ds_id] = {
                **metadata
            }
            parsed_data_source_dict[ds_id] = {
                'schema': {
                    'tables': tables,
                    'columns': columns
                },
                'queries': {
                    'custom_sql': custom_sql,
                    'initial_sql': initial_sql,
                    'parsed_custom_sql': parsed_custom_sql,
                    'parsed_initial_sql': parsed_initial_sql
                }
            }

            test = {
                'id': ds_id,
                'XML_tables': tables,
                'SQL_tables': parsed_custom_sql.get('source_tables')
            }
            pp(test)

    # print('METADATA DICT...')
    # pp(metadata_dict)

    # print('CUSTOM SQL DICT...')
    # pp(parsed_data_source_dict)

    # TODO: Delete /tmp/ files after parsing?


def authenticate_tableau():
    #
    # AUTHENTICATION
    #
    load_dotenv()
    TOKEN_NAME = os.getenv('TABLEAU_PAT_NAME')
    TOKEN = os.getenv('TABLEAU_PAT')
    SERVER_URL = os.getenv('TABLEAU_SERVER_URL')

    auth = TSC.PersonalAccessTokenAuth(
        token_name=TOKEN_NAME,
        personal_access_token=TOKEN
    )

    server = TSC.Server(
        server_address=SERVER_URL,
        use_server_version=True
    )

    return auth, server


def download_data_sources(tableau_server, data_source_ids_list):
    # Store path to each file
    file_path_dict = {}

    # Download each data source on Tableau Server and parse for its Custom SQL and additional Metadata
    for ds_id in data_source_ids_list:
        zipped_ds_path = tableau_server.datasources.download(
            ds_id,
            filepath='./tmp',
            include_extract=False
        )

        # Packaged Datasource (.tdsx) files are zipped archives. Here we extract the Datasource (.tds) only.
        unzipped_ds_path = None
        with zipfile.ZipFile(zipped_ds_path) as packaged_data_source:
            for ds_file in packaged_data_source.namelist():
                if '.tds' in ds_file:
                    unzipped_ds_path = packaged_data_source.extract(ds_file, './tmp')
                    print(f'Extracting .tds file from...\t {ds_file}')
                else:
                    print(f'No .tds file found in...\t\t {ds_file}')

        # Convert .tds to .xml
        tds_file = Path(os.getcwd() + '/' + unzipped_ds_path)
        tds_as_xml = tds_file.rename(tds_file.with_suffix('.xml'))

        # Add path to dict
        file_path_dict[ds_id] = tds_as_xml

        # Remove .tdsx from temporary directory
        tmp_dir = os.getcwd() + '/tmp'
        for f in os.listdir(Path(tmp_dir)):
            if '.tdsx' in f:
                os.remove(Path(tmp_dir + '/' + f))

    return file_path_dict


def get_initial_sql(initial_sql_xml):
    initial_sql_list = []
    for conn in initial_sql_xml:
        # Store Initial SQL query
        if bool(conn.attrib.get('one-time-sql')):
            initial_sql_list.append(conn.attrib.get('one-time-sql'))

    return initial_sql_list


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
            connection_metadata['connection_ids'] = conn.attrib.get('id')
            connection_metadata['connection_dbname'] = conn.attrib.get('dbname')
            connection_metadata['connection_schema'] = conn.attrib.get('schema')
            connection_metadata['connection_warehouse'] = conn.attrib.get('warehouse')
            if bool(conn.attrib.get('one-time-sql')):
                connection_metadata['has_initial_sql'] = True

    connection_metadata['connections'] = connection_types_list

    return connection_metadata


def get_custom_sql(custom_sql_xml):
    # Get Custom SQL
    custom_sql_list = []
    for sql_item in custom_sql_xml:
        if sql_item.text is not None:
            if str.lstrip(sql_item.text, '\n|" "') != '':
                custom_sql_list.append(sql_item.text)

    return custom_sql_list


def format_custom_sql(raw_sql_list):
    # Format SQL
    formatted_sql = []
    for raw_sql in raw_sql_list:
        fmt_sql = sqlparse.format(
            raw_sql,
            reindent=True,
            strip_comments=True,
            keyword_case='upper',
            comma_first=True
        )
        # Split SQL by statement (a phrase ending in a semicolon)
        split_statement = sqlparse.split(fmt_sql)

        formatted_sql.append(split_statement)

    return formatted_sql


def parse_custom_sql(clean_sql_list):
    table_metadata_dict = {}
    for q_list in clean_sql_list:
        for q in q_list:
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


def get_tables_from_xml(tables_xml):

    # Get table names
    tables_dict = {}
    for relation in tables_xml:
        if str(relation.attrib.get('type')) == 'table':
            tables_dict[relation.attrib.get('name')] = relation.attrib.get('table')

    return tables_dict


def get_columns_from_xml(columns_xml):
    # Get Column names, aliases, and calculations
    columns_dict = {}
    for col in columns_xml:
        name = col.attrib.get('name').strip('[|]')
        alias = col.attrib.get('caption')
        datatype = col.attrib.get('datatype')
        formula = None
        for calc in col.iter('calculation'):
            formula = calc.attrib.get('formula')

        columns_dict[name] = {
            'alias': alias,
            'datatype': datatype,
            'calculation': formula
        }

    return columns_dict


if __name__ == "__main__":
    parse_tableau()