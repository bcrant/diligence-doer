import os
import zipfile
import tableauserverclient as tsc
from dotenv import load_dotenv


def parse_tableau():
    #
    # Authentication
    #
    load_dotenv()
    TOKEN_NAME = os.getenv('TABLEAU_PAT_NAME')
    TOKEN = os.getenv('TABLEAU_PAT')
    SERVER_URL = os.getenv('TABLEAU_SERVER_URL')

    tableau_auth = tsc.PersonalAccessTokenAuth(
        token_name=TOKEN_NAME,
        personal_access_token=TOKEN
    )
    server = tsc.Server(SERVER_URL, use_server_version=True)

    #
    # Download all data sources on Tableau Server
    #
    with server.auth.sign_in_with_personal_access_token(tableau_auth):
        all_data_sources, pagination_item = server.datasources.get()
        print('There are {} data sources on site: '.format(pagination_item.total_available))

        all_datasource_ids = [datasource.id for datasource in all_data_sources]
        print(all_datasource_ids)
        # for ds_id in all_datasource_ids:

        zipped_ds_path = server.datasources.download(
            all_datasource_ids[5],
            filepath='./tmp',
            include_extract=False
        )

        # Packaged Datasource (.tdsx) files are zipped archives. Here we extract the Datasource (.tds) only.
        unzipped_ds_path = None
        with zipfile.ZipFile(zipped_ds_path) as packaged_data_source:
            for ds_file in packaged_data_source.namelist():
                if '.tds' in ds_file:
                    unzipped_ds_path = packaged_data_source.extract(ds_file, './tmp')
                    print(f'Extracting Tableau Data Source from .tdsx...')
                else:
                    print(f'No Tableau Data Source file found in .tdsx.')

        #
        # Parse Data Source for its Custom SQL
        #
        with open(unzipped_ds_path) as f:
            ds_f = f.read()

            tableau_custom_sql = ds_f\
                .split('<relation')[1]\
                .split('\'>')[1]\
                .split('</relation>')[0]
            print(tableau_custom_sql)
            print('\n\n')


if __name__ == "__main__":
    parse_tableau()

# # EXPLORE DATASOURCE ATTRIBUTES
#
# for datasource in all_datasources:
#     print('datasource.id: ', datasource.id)
#     print('datasource.name: ', datasource.name)
#     print('datasource.datasource_type: ', datasource.datasource_type)
#
#     server.datasources.populate_connections(datasource)
#
#     for connection in datasource.connections:
#         print('connection.datasource_id: ', connection.datasource_id)
#         print('connection.datasource_name: ',  connection.datasource_name)
#         print('connection.id: ',  connection.id)
#         print('connection.connection_type: ',  connection.connection_type)
#         print('connection.username: ',  connection.username)
#         print('connection.password: ',  connection.password)
#         print('connection.embed_password: ',  connection.embed_password)
#         print('connection.server_address: ',  connection.server_address)
#         print('connection.server_port: ',  connection.server_port)
#
#     print('\n\n')
