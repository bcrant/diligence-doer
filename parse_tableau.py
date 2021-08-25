import os
import zipfile
from pathlib import Path
import xml.etree.ElementTree as ET
import tableauserverclient as TSC
from dotenv import load_dotenv


def parse_tableau():
    #
    # AUTHENTICATION
    #
    load_dotenv()
    TOKEN_NAME = os.getenv('TABLEAU_PAT_NAME')
    TOKEN = os.getenv('TABLEAU_PAT')
    SERVER_URL = os.getenv('TABLEAU_SERVER_URL')

    AUTHENTICATION = TSC.PersonalAccessTokenAuth(
        token_name=TOKEN_NAME,
        personal_access_token=TOKEN
    )

    SERVER = TSC.Server(
        server_address=SERVER_URL,
        use_server_version=True
    )

    #
    # Get metadata about all data sources on Tableau Server
    #
    metadata_dict = {}
    with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):
        all_data_sources, pagination_item = SERVER.datasources.get()
        print('There are {} data sources on site...'.format(pagination_item.total_available))

        for datasource in all_data_sources[10:20]:
            metadata_dict[datasource.id] = {
                'datasource_id': datasource.id,
                'datasource_name': datasource.name,
                'datasource_type': datasource.datasource_type
            }

        #
        # Download each data source on Tableau Server
        #
        custom_sql_dict = {}
        for ds_id in metadata_dict.keys():
            zipped_ds_path = SERVER.datasources.download(
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

            # Remove .tdsx from temporary directory
            tmp_dir = os.getcwd() + '/tmp'
            for f in os.listdir(Path(tmp_dir)):
                if '.tdsx' in f:
                    os.remove(Path(tmp_dir + '/' + f))

            #
            # Parse each data source for its Custom SQL and store in dictionary
            #
            with open(tds_as_xml) as f:
                tree = ET.parse(f)

            root = tree.getroot()

            # Add more metadata
            all_xml_elements = dict.fromkeys(
                [
                    elem.tag
                    for elem in root.iter()
                ]
            )
            unique_xml_list = list(all_xml_elements.keys())
            metadata_dict.get(ds_id)['xml_elements'] = unique_xml_list

            # Initialize placeholder lists to collect values from XML
            tables = []
            connections = []
            initial_sql = []
            custom_sql = []

            #
            # Connection properties of data source xml
            #
            for conn in root.iter('connection'):
                connection_type = conn.attrib.get('class')
                if connection_type != 'federated':
                    connections.append(connection_type)

                if connection_type in ['snowflake', 'redshift']:
                    # Store Initial SQL query
                    if bool(conn.attrib.get('one-time-sql')):
                        metadata_dict.get(ds_id)['has_initial_sql'] = True
                        initial_sql.append(conn.attrib.get('one-time-sql'))

                    # Add more metadata to context
                    metadata_dict.get(ds_id)['server_address'] = conn.attrib.get('server')
                    metadata_dict.get(ds_id)['server_port'] = conn.attrib.get('port')
                    metadata_dict.get(ds_id)['connection_ids'] = conn.attrib.get('id')
                    metadata_dict.get(ds_id)['connection_dbname'] = conn.attrib.get('dbname')
                    metadata_dict.get(ds_id)['connection_schema'] = conn.attrib.get('schema')
                    metadata_dict.get(ds_id)['connection_warehouse'] = conn.attrib.get('warehouse')

            #
            # Relation properties of data source xml
            #
            for rel in root.iter('relation'):
                # Get Custom SQL
                if rel.text is not None:
                    if str.lstrip(rel.text, '\n|" "') != '':
                        custom_sql.append(rel.text)
                if bool(rel.attrib.get('table')):
                    tables.append(rel.attrib.get('table'))

            #
            # Column names, aliases, and calculations
            #
            columns = {}
            for col in root.iter('column'):
                name = col.attrib.get('name')
                alias = col.attrib.get('caption')
                datatype = col.attrib.get('datatype')
                for calc in col.iter('calculation'):
                    formula = calc.attrib.get('formula')

                columns[name] = {
                    'alias': alias,
                    'datatype': datatype,
                    'calculation': formula
                }

            custom_sql_dict[ds_id] = {
                'columns': columns,
                'custom_sql': custom_sql,
                'initial_sql': initial_sql,
                'tables': tables,
                'connections': connections
            }

        # TODO: Parse the Custom SQL and Initial SQL to uncover all database table dependencies

        # TODO: Delete /tmp/ files after parsing?


if __name__ == "__main__":
    parse_tableau()
