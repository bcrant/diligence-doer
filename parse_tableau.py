import os
import pprint
import zipfile
import tableauserverclient as tsc
from dotenv import load_dotenv

#
# AUTHENTICATION
#
load_dotenv()
TOKEN_NAME = os.getenv('TABLEAU_PAT_NAME')
TOKEN = os.getenv('TABLEAU_PAT')
SERVER_URL = os.getenv('Tableau.SERVER_URL')


class Tableau:
    AUTHENTICATION = tsc.PersonalAccessTokenAuth(
        token_name=TOKEN_NAME,
        personal_access_token=TOKEN
    )
    SERVER = tsc.Server(
        server_address=SERVER_URL, 
        use_server_version=True
    )


def parse_tableau():
    #
    # Get metadata about all data sources on Tableau Server
    #
    datasource_dict = {}
    with Tableau.SERVER.auth.sign_in_with_personal_access_token(Tableau.AUTHENTICATION):
        all_data_sources, pagination_item = Tableau.SERVER.datasources.get()
        print('There are {} data sources on site: '.format(pagination_item.total_available))

        for datasource in all_data_sources[0:10]:
            # Populate all metadata about the datasource's connections
            Tableau.SERVER.datasources.populate_connections(datasource)

            # TODO: Investigate if we need to handle case where
            #       data source that has multiple connection IDs...
            #       (ed724163-6daf-4aee-be3c-8b8dfe3890e9, 077e71fd-3bf0-47f2-9e3a-346b2fef9461)
            datasource_dict[datasource.id] = {
                'datasource_id': datasource.id,
                'datasource_name': datasource.name,
                'datasource_type': datasource.datasource_type,
                'server_address': datasource.connections[0].server_address,
                'server_port': datasource.connections[0].server_port,
                'connection_ids': datasource.connections[0].id,
            }

    #
    # Download each data source on Tableau Server
    #
    for ds_id in datasource_dict.keys():
        zipped_ds_path = Tableau.SERVER.datasources.download(
            datasource_dict.get(ds_id).get('datasource_name'),
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

        #
        # TODO: What happens if data source does not have custom SQL?
        #       What happens if it has multiple custom SQL?
        #


if __name__ == "__main__":
    parse_tableau()
