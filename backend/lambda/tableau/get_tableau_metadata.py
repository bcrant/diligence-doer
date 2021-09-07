# import tableauserverclient as TSC
from metadata_queries import MetadataQueries
from utils.authentication import authenticate_tableau
from utils.dynamodb import write_to_dynamodb
from utils.helpers import *


def get_metadata():
    # Tableau Authentication
    AUTHENTICATION, SERVER = authenticate_tableau()

    # Log in to Tableau Server and query the Metadata API
    with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):
        #
        # DatabaseTables to Dashboards
        #
        tables_to_dashboards = SERVER.metadata\
            .query(MetadataQueries.DATABASE_TABLES_TO_DASHBOARDS)\
            .get('data')\
            .get('databaseTables')

        clean_tables_to_dashboards(tables_to_dashboards)

        # published_datasources = SERVER.metadata.query(MetadataQueries.PUBLISHED_DATASOURCES)
        # pp(published_datasources['data'])

        # embedded_datasources = SERVER.metadata.query(MetadataQueries.EMBEDDED_DATASOURCES)
        # pp(embedded_datasources['data'])

        # workbooks = SERVER.metadata.query(MetadataQueries.WORKBOOKS)
        # pp(workbooks['data'])

        # databases = SERVER.metadata.query(MetadataQueries.DATABASES)
        # pp(databases['data'])

        # database_tables = SERVER.metadata.query(MetadataQueries.DATABASE_TABLES)
        # pp(database_tables['data'])

        # # workbook_fields = TSC.Pager(SERVER.metadata.query(MetadataQueries.WORKBOOK_FIELDS))
        # workbook_fields = SERVER.metadata.query(MetadataQueries.WORKBOOK_FIELDS)
        # pp(workbook_fields)

        # custom_sql = SERVER.metadata.query(MetadataQueries.CUSTOM_SQL_TABLES)
        # pp(custom_sql['data'])


def clean_tables_to_dashboards(tables_to_dashboards_dict):
    # Clean keys and values to make matching easier
    cleaned_dicts_list = list()
    for table_dict in tables_to_dashboards_dict:

        # Filter out text files
        text_filters = ('hyper', 'webdata-direct', 'textscan', 'excel-direct', 'google-sheets')
        if table_dict.get('connectionType') not in text_filters:

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
            table_dict['pk'] = table_dict.get('luid')
            table_dict.pop('luid')

            # Move column names into list
            cols = list()
            if table_dict.get('columns') is not None:
                for col_dict in table_dict.get('columns'):
                    col_name = col_dict.get('name')
                    cols.append(col_name)
                    # TODO: Add support for columns. Structuring for Table names for now...
                    # # Add all possible combos of schema, table, and column names for matching
                    # cols.extend([
                    #     col_name,
                    #     str(table_dict.get('fullName') + '.' + col_name),
                    #     str(table_dict.get('table_name') + '.' + col_name)
                    # ])
            table_dict['columns'] = cols

            # pp(table_dict)
            # write_to_dynamodb(
            #     record=table_dict,
            #     pk='pk'
            # )

            cleaned_dicts_list.append(table_dict)

    table_to_dash = map_tables_to_dashboards(cleaned_dicts_list)
    pp(table_to_dash)

    return table_to_dash


def map_tables_to_dashboards(cleaned_tables_to_dashboard_list):
    map_dict = dict()
    for clean_table in cleaned_tables_to_dashboard_list:
        if len(clean_table.get('downstreamDashboards')) >= 1:
            table_full_name = clean_table.get('fullName')

            if map_dict.get(table_full_name) is not None:
                for dash in clean_table.get('downstreamDashboards'):
                    if dash.get('name') not in map_dict.get(table_full_name):
                        map_dict.get(table_full_name).append(dash.get('name'))

            else:
                map_dict[table_full_name] = list()
                for dash in clean_table.get('downstreamDashboards'):
                    if dash.get('name') not in map_dict.get(table_full_name):
                        map_dict.get(table_full_name).append(dash.get('name'))

    return map_dict


if __name__ == "__main__":
    get_metadata()
