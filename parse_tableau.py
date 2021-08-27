import os
import pprint
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET
import tableauserverclient as TSC
from dotenv import load_dotenv


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
        for datasource in all_data_sources[10:11]:
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
            with open(ds_path) as f:
                tree = ET.parse(f)
            root = tree.getroot()

            # METADATA - Connection properties of data source xml
            connections = root.iter('connection')
            metadata = get_connection_metadata(connections)
            initial_sql = get_initial_sql(connections)

            # SCHEMA - Relation properties of data source xml
            relations = root.iter('relation')
            custom_sql = get_custom_sql(relations)
            tables = get_tables(relations)
            columns = get_columns(root.iter('column'))

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
                    'initial_sql': initial_sql
                }
            }

    print('METADATA DICT...')
    pprint.pprint(metadata_dict)

    print('CUSTOM SQL DICT...')
    pprint.pprint(parsed_data_source_dict)

    # TODO: Parse the Custom SQL and Initial SQL to uncover all database table dependencies
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
                    print(f'Extracting Tableau Data Source from .tdsx... {ds_file}')
                else:
                    print(f'No Tableau Data Source file found in .tdsx... {ds_file}')

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


def get_tables(tables_xml):
    # Get table names
    tables_list = []
    for table_item in tables_xml:
        if table_item.text is not None:
            if str.lstrip(table_item.text, '\n|" "') != '':
                tables_list.append(table_item.text)

    return tables_list


def get_columns(columns_xml):
    # Get Column names, aliases, and calculations
    columns_dict = {}
    for col in columns_xml:
        name = col.attrib.get('name')
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
