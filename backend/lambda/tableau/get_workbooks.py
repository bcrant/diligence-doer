import pprint
import zipfile
from parse_tableau import parse_tableau
from utils.authentication import authenticate_tableau
from utils.dynamodb import write_to_dynamodb
from utils.helpers import *


def get_workbooks():
    # Parse all datasources on Tableau Server
    # datasources_dict = parse_tableau()
    # print('DATASOURCES DICT...')
    # pp(datasources_dict)

    connections_list = list()

    # Tableau Authentication
    AUTHENTICATION, SERVER = authenticate_tableau()

    # Log in to Tableau Server and query all Workbooks on Server
    with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):
        all_workbooks, pagination_item = SERVER.workbooks.get()
        print('There are {} workbooks on site...'.format(pagination_item.total_available))

        # Download and each Workbook, extract the .twb files and store path to each .twb in dictionary.
        workbook_path_dict = download_workbooks(SERVER, all_workbooks)


def download_workbooks(tableau_server, workbooks):
    # Store path
    file_path_dict = dict()

    for workbook in workbooks:
        zipped_wb_path = tableau_server.workbooks.download(
            workbook.id,
            filepath='./tmp',
            include_extract=False
        )

        # Packaged Workbooks (.twbx) files are zipped archives. Here we extract the Workbook (.twb) only.
        with zipfile.ZipFile(zipped_wb_path) as packaged_data_source:
            print('namelist: ', packaged_data_source.namelist())
            for wb_file in packaged_data_source.namelist():
                if '.twb' in wb_file:
                    unzipped_wb_path = packaged_data_source.extract(wb_file, './tmp')
                    print(f'Extracting {wb_file} file from {workbook.name}...')

                    # Convert .twb to .xml
                    xml_path = convert_tableau_file_to_xml(unzipped_wb_path)

                    # Add path to dict
                    file_path_dict[workbook.id] = xml_path

    # Remove all .twbx from temporary directory
    delete_tmp_files_of_type('twbx', 'tmp')

    return file_path_dict

        # for workbook in all_workbooks:
            # # Populate connection information about each workbook
            # SERVER.workbooks.populate_connections(workbook)
            #
            # connection_dicts_list = list()
            # for connection in workbook.connections:
            #     # print('[WB] After:\t\t', vars(connection))
            #     log('[WB] connection.datasource_id', connection.datasource_id)
            #     log('[WB] connection.datasource_name', connection.datasource_name)
            #     log('[WB] connection.id', connection.id)
            #
            #     connection_dict = {
            #         'datasource_id': connection.datasource_id,
            #         'datasource_name': connection.datasource_name,
            #         'connection_id': connection.id,
            #         'workbook_name': workbook.name,
            #         'workbook_id': workbook.id
            #     }
            #
            #     connection_dicts_list.append(connection_dict)
            #
            # connections_list.extend(connection_dicts_list)

    # print(connections_list)
    # for i in connections_list:
    #     print(i.get('datasource_id'))


# for c in workbook.connections:
#     log('[WB] conn after', vars(c))

# # Store each datasource id in a list
# wb_datasource_ids_list = [
#     datasource.datasource_id
#     for datasource in workbook.connections
# ]
#
# log('wb_datasource_ids_list', wb_datasource_ids_list)
#
# # Get entire datasource object from each datasource id in workbook
# for ds_id in wb_datasource_ids_list:
#     print('\n\n')
#     log('datasource_ids_list', wb_datasource_ids_list)
#     log('ds_id', ds_id)
#     log('workbook_id', workbook.id)
#     log('workbook_name', workbook.name)
#     log('workbook_url', workbook.webpage_url)
#
#     # try:
#     datasource_object = datasources_dict.get(ds_id)
#     log('datasource_object', datasource_object)
#
#     if datasource_object is not None:
#         # Make denormalized id from datasource and workbook ids
#         ds_wb_id = str(ds_id + '_' + workbook.id)
#         log('ds_wb_id', ds_wb_id)
#
#         # Combine workbook and datasource information
#         tableau_output_dict = {
#             'pk': ds_wb_id,
#             ds_wb_id: {
#                 'workbook_id': workbook.id,
#                 'workbook_name': workbook.name,
#                 'workbook_url': workbook.webpage_url,
#                 **datasource_object
#             }
#         }
#
#         log('tableau_output_dict', tableau_output_dict)
#
#         # Write 1:1 objects to Dynamodb
#         write_to_dynamodb(record=tableau_output_dict, pk='pk')
#
#     else:
#         print('Datasource not found. Likely a text file and not a database connection. Continuing...')
#
#     # except TypeError:
#     #     continue


if __name__ == "__main__":
    get_workbooks()
