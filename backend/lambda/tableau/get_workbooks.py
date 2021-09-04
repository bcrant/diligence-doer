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
    workbook_dict = {}

    # Log in to Tableau Server and query all Data Sources on Server
    with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):
        all_workbooks, pagination_item = SERVER.workbooks.get()
        print('There are {} workbooks on site...'.format(pagination_item.total_available))

        for workbook in all_workbooks[0:50]:
            # Populate connection information about each workbook
            SERVER.workbooks.populate_connections(workbook)

            workbook_connection_ids = [
                connection.id
                for connection in workbook.connections
            ]

            workbook_connection_names = [
                connection.datasource_name
                for connection in workbook.connections
            ]

            workbook_dict[workbook.id] = {
                'workbook_name': workbook.name,
                'connections': {
                    'connection_id': ,
                    'connection_name':
                }
            }
            print('\n\n')
            # pprint.pprint(vars(workbook))


if __name__ == "__main__":
    get_workbooks()
