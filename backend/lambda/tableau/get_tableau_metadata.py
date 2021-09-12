from utils.authentication import authenticate_tableau
from utils.dynamodb import write_to_dynamodb
from utils.helpers import *
from utils.queries import TableauMetadataQueries


def get_metadata():
    # Tableau Authentication
    AUTHENTICATION, SERVER = authenticate_tableau()

    # Log in to Tableau Server and query the Metadata API
    with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):
        #
        # DatabaseTables to Dashboards
        #
        tables_to_dashboards = SERVER.metadata\
            .query(TableauMetadataQueries.DATABASE_TABLES_TO_DASHBOARDS)\
            .get('data')\
            .get('databaseTables')

        cleaned_tables = clean_tables_to_dashboards(tables_to_dashboards)
        mapped_tables = map_tables_to_dashboards(cleaned_tables)

        for table_dict in mapped_tables:
            pp(table_dict)
        #     write_to_dynamodb(
        #         record=table_dict,
        #         pk='pk'
        #     )


def clean_tables_to_dashboards(tables_to_dashboards_dict):
    # Clean keys and values to make matching easier
    cleaned_dicts_list = list()
    for table_dict in tables_to_dashboards_dict:

        # Filter out text files
        text_filters = ('hyper', 'webdata-direct', 'textscan', 'excel-direct', 'google-sheets')
        if table_dict.get('connectionType') not in text_filters:

            # Strip brackets from table fullName's
            table_dict['fullName'] = strip_brackets(table_dict.get('fullName')).lower()

            # Smell test schema response
            if table_dict.get('schema') not in table_dict.get('fullName'):
                table_dict['schema'] = table_dict.get('fullName').split('.')[0]

            # Rename keys
            table_dict['table_name'] = table_dict.get('name')
            table_dict.pop('name')
            table_dict['schema_name'] = table_dict.get('schema')
            table_dict.pop('schema')

            # Move column names into list
            if table_dict.get('columns') is not None:
                table_dict['columns'] = [
                    col_dict.get('name').lower()
                    for col_dict in table_dict.get('columns')
                ]

            cleaned_dicts_list.append(table_dict)

    return cleaned_dicts_list


def map_tables_to_dashboards(cleaned_tables_to_dashboard_list):
    list_of_map_dicts = list()
    for clean_table in cleaned_tables_to_dashboard_list:

        if len(clean_table.get('downstreamDashboards')) >= 1:
            table_full_name = clean_table.get('fullName')

            map_dict = {
                'pk': table_full_name,
                'tableau': {
                    'columns': clean_table.get('columns'),
                    'dashboards': dict()
                },
                'github': dict()
            }

            dashboards = map_dict.get('tableau').get('dashboards')
            for dash in clean_table.get('downstreamDashboards'):
                if dashboards is not None:
                    if dash.get('name') not in list(dashboards.keys()):
                        dashboards[dash.get('name')] = str(
                            'https://tableau_server_url.com/#/views/' + dash.get('path')
                        )
                else:
                    dashboards = dict()
                    if dash.get('name') not in list(dashboards.keys()):
                        dashboards[dash.get('name')] = str(
                            'https://tableau_server_url.com/#/views/' + dash.get('path')
                        )

            list_of_map_dicts.append(map_dict)

    return list_of_map_dicts


def clean_published_datasources(published_datasources):
    cleaned_datasources_list = list()
    for ds in published_datasources:
        if ds.get('downstreamDashboards') is not None:
            fields = list(strip_brackets(f.get('fullyQualifiedName')).lower() for f in ds.get('fields'))
            tables = list(strip_brackets(t.get('fullName')).lower() for t in ds.get('upstreamTables'))
            cleaned_datasources_list.append({
                'fields': fields,
                'tables': tables
            })

    return cleaned_datasources_list


def unused_queries():
    print()
    # published_datasources = SERVER.metadata\
    #     .query(TableauMetadataQueries.PUBLISHED_DATASOURCES)\
    #     .get('data')\
    #     .get('publishedDatasources')
    # pp(published_datasources)

    # cleaned_datasources = clean_published_datasources(published_datasources)
    # pp(cleaned_datasources)

    # embedded_datasources = SERVER.metadata.query(TableauMetadataQueries.EMBEDDED_DATASOURCES)
    # pp(embedded_datasources['data'])

    # workbooks = SERVER.metadata.query(TableauMetadataQueries.WORKBOOKS)
    # pp(workbooks['data'])

    # databases = SERVER.metadata.query(TableauMetadataQueries.DATABASES)
    # pp(databases['data'])

    # database_tables = SERVER.metadata.query(TableauMetadataQueries.DATABASE_TABLES)
    # pp(database_tables['data'])

    # # workbook_fields = TSC.Pager(SERVER.metadata.query(TableauMetadataQueries.WORKBOOK_FIELDS))
    # workbook_fields = SERVER.metadata.query(TableauMetadataQueries.WORKBOOK_FIELDS)
    # pp(workbook_fields)

    # custom_sql = SERVER.metadata.query(TableauMetadataQueries.CUSTOM_SQL_TO_DASHBOARDS)
    # pp(custom_sql['data'])


if __name__ == "__main__":
    get_metadata()
