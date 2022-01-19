#
# GraphQL Queries for Tableau Metadata API
#

class TableauMetadataQueries:
    CUSTOM_SQL_TO_DASHBOARDS = '''
    query custom_sql_tables {
        customSQLTablesConnection(first: 50){
            nodes {               
                # name
                # query

                database {
                    name
                    connectionType
                }

                tables {
                    name
                    schema
                }

                columns {
                    name
                }


                downstreamDashboards {
                    __typename
                    name
                    path
                }
            }
        }
    }        
    '''

    DATABASE_TABLES_TO_DASHBOARDS = '''
    query table_fields {
        databaseTables {
            name
            fullName
            # NOTE: Take schema value with a grain of salt. Give "name" and "fullName" precedence.
            # Added logic to get_tableau_metadata to handle. 
            schema
            connectionType

            columns {
                name
            }

            downstreamDashboards {
                name
                path
            }
        }
    } 
    '''

    DATABASE_TABLES = '''
    query database_tables {
        databaseTables {
            luid
            id
            name
            schema
            fullName
            connectionType
        }
    }
    '''

    DATABASES = '''
    query databases {
        databases {
            luid
            id
            name
            connectionType
        }
    }
    '''

    WORKBOOK_FIELDS = '''
    query workbook_fields {
        # workbooks (filter: { projectNameWithin: ["BRLS", "Live", "TRLS", "URLS"] } ) {
        # workbooks (filter: { projectNameWithin: ["URLS"] } ) {
        workbooks {
            __typename
            name
            projectName

            sheets {
                __typename
                name

                sheetFieldInstances {
                    __typename
                    name           
                    fullyQualifiedName

                    datasource {
                        __typename
                        name
                        ... on EmbeddedDatasource {
                            upstreamDatasources {
                                __typename
                                name                    
                            }                        
                        }   
                    }
                }
            }
        }
    }    
    '''

    PUBLISHED_DATASOURCES = '''
    query published_datasources {
        publishedDatasources {
            name
            uri
            hasExtracts

            upstreamTables {
                fullName
            }

            downstreamDashboards {
                name
            }

            fields {
                name
                fullyQualifiedName
            }       

        }
    }
    '''

    EMBEDDED_DATASOURCES = '''
    query embedded_datasources {
        embeddedDatasources {
            # __typename
            name
            # id

            # fields {
            #     name
            #     fullyQualifiedName
            # }

            downstreamDashboards {
                # __typename
                name
            }       
        }
    }
    '''

    WORKBOOKS = '''
    query workbooks{
        workbooks {
            id
            luid
            name
            uri
            upstreamDatasources {
                id
                luid
                name
            }
            upstreamDatabases {
                id
                luid
                name
                connectionType
            }
            upstreamTables {
                id
                luid
                name
            }
        }
    }    
    '''

