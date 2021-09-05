import os
import pprint
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


def get_workbooks():
    # Tableau Authentication
    AUTHENTICATION, SERVER = authenticate_tableau()

    # Initialize dictionaries to store outputs
    workbooks_dict = {}

    # Log in to Tableau Server and query all Workbooks on Server
    with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):
        all_workbooks, pagination_item = SERVER.workbooks.get()
        print('There are {} workbooks on site...'.format(pagination_item.total_available))

        for workbook in all_workbooks[30:50]:
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

            # Combine workbook and datasource connection information
            workbooks_dict[workbook.id] = {
                'workbook_id': workbook.id,
                'workbook_name': workbook.name,
                'datasource_connections': wb_connections_list_of_dicts
            }

    pp(workbooks_dict)


if __name__ == "__main__":
    get_workbooks()
