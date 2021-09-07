# import tableauserverclient as TSC
from metadata_queries import MetadataQueries
from utils.authentication import authenticate_tableau
from utils.helpers import *


def get_metadata():
    # Tableau Authentication
    AUTHENTICATION, SERVER = authenticate_tableau()

    # Log in to Tableau Server and query the Metadata API
    with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):
        # Query the Metadata API and store the response in resp

        # workbooks = SERVER.metadata.query(MetadataQueries.WORKBOOKS)
        # pp(workbooks['data'])

        # databases = SERVER.metadata.query(MetadataQueries.DATABASES)
        # pp(databases['data'])

        # database_tables = SERVER.metadata.query(MetadataQueries.DATABASE_TABLES)
        # pp(database_tables['data'])

        # workbook_fields = TSC.Pager(SERVER.metadata.query(MetadataQueries.WORKBOOK_FIELDS))
        # workbook_fields = SERVER.metadata.query(MetadataQueries.WORKBOOK_FIELDS)
        # pp(workbook_fields)

        tables_to_dashboards = SERVER.metadata.query(MetadataQueries.DATABASE_TABLES_TO_DASHBOARDS)\
            .get('data')\
            .get('databaseTables')

        # Clean dict
        for table_dict in tables_to_dashboards:
            # Strip brackets from table fullName's
            table_dict['fullName'] = strip_brackets(table_dict.get('fullName'))

            # Smell test schema response
            if table_dict.get('schema') not in table_dict.get('fullName'):
                table_dict['schema'] = table_dict.get('fullName').split('.')[0]

            # Rename keys
            table_dict['table_name'] = table_dict.get('name')
            table_dict.pop('name')
            table_dict['schema_name'] = table_dict.get('schema')
            table_dict.pop('schema')

            # Move column names into list
            cols = list()
            if table_dict.get('columns') is not None:
                for col_dict in table_dict.get('columns'):
                    col_name = col_dict.get('name')
                    # Add all possible combos of schema, table, and column names for matching
                    cols.extend([
                        col_name,
                        str(table_dict.get('fullName') + '.' + col_name),
                        str(table_dict.get('table_name') + '.' + col_name)
                    ])
            table_dict['columns'] = cols

            pp(table_dict)
        # pp(tables_to_dashboards)

        # for table_dict in tables_to_dashboards:
        #     print(table_dict)

        # custom_sql = SERVER.metadata.query(MetadataQueries.CUSTOM_SQL_TABLES)
        # pp(custom_sql['data'])

        # published_datasources = SERVER.metadata.query(MetadataQueries.PUBLISHED_DATASOURCES)
        # pp(published_datasources['data'])

        # embedded_datasources = SERVER.metadata.query(MetadataQueries.EMBEDDED_DATASOURCES)
        # pp(embedded_datasources['data'])


if __name__ == "__main__":
    get_metadata()
