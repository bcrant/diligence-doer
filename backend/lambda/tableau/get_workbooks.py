from utils.authentication import authenticate_tableau
from utils.dynamodb import write_to_dynamodb


def get_workbooks():
    # Tableau Authentication
    AUTHENTICATION, SERVER = authenticate_tableau()

    # Initialize dictionaries to store outputs
    workbooks_dict = {'pk': 'workbook_to_datasource'}

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

            unique_connection_ids_list = list()
            for conn in wb_connections_list_of_dicts:
                unique_connection_ids_list.append(
                    conn.get('connection_id')
                )

            # Combine workbook and datasource connection information
            workbooks_dict[workbook.id] = {
                'workbook_id': workbook.id,
                'workbook_name': workbook.name,
                'connection_metadata': wb_connections_list_of_dicts,
                'datasource_ids_list': unique_connection_ids_list
            }

    # Write output to AWS DynamoDB
    write_to_dynamodb(record=workbooks_dict, pk='pk')


if __name__ == "__main__":
    get_workbooks()
