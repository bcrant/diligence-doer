from utils.authentication import authenticate_tableau
from utils.dynamodb import write_to_dynamodb
from utils.helpers import pp


def get_workbooks():
    # Tableau Authentication
    AUTHENTICATION, SERVER = authenticate_tableau()

    # Initialize dictionaries to store outputs
    workbooks_dict = {'pk': 'workbook_to_datasource'}
    ds_to_wb_dict = {'pk': 'datasource_to_workbook'}

    # Log in to Tableau Server and query all Workbooks on Server
    with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):
        all_workbooks, pagination_item = SERVER.workbooks.get()
        print('There are {} workbooks on site...'.format(pagination_item.total_available))

        for workbook in all_workbooks:
            # Populate datasource connection information about each workbook
            SERVER.workbooks.populate_connections(workbook)

            # Store each connection information in a dictionary
            wb_connections_list_of_dicts = list()
            for connection in workbook.connections:
                wb_connections_list_of_dicts.append({
                    'datasource_id': connection.datasource_id,
                    'datasource_name': connection.datasource_name,
                    'connection_id': connection.id
                })

            unique_datasource_ids_list = list()
            for ds in wb_connections_list_of_dicts:
                ds_id = ds.get('datasource_id')
                # Build workbook_to_datasource dict
                unique_datasource_ids_list.append(ds_id)

                # Build 1 to 1 datasource_to_workbook_dict
                ds_to_wb_dict[ds_id] = workbook.id

            # Combine workbook and datasource connection information
            workbooks_dict[workbook.id] = {
                'workbook_id': workbook.id,
                'workbook_name': workbook.name,
                'connection_metadata': wb_connections_list_of_dicts,
                'datasource_ids_list': unique_datasource_ids_list
            }

    # Write Workbook metadata to Dynamodb
    # write_to_dynamodb(record=workbooks_dict, pk='pk')

    # Write 1 to 1 datasource_id to workbook_id items to Dynamodb
    write_to_dynamodb(record=ds_to_wb_dict, pk='pk')


if __name__ == "__main__":
    get_workbooks()
