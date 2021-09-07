class MetadataQueries:

    CUSTOM_SQL_TABLES = '''
        query custom_sql_tables {
            customSQLTablesConnection(first: 10){
                nodes {               
                    name
                    id
                    query
    
                    database {
                        name
                        id
                        luid
                        connectionType
                    }
    
                    tables {
                        name
                        id
                        luid
                        schema
                    }
    
                    # columns {
                    #     name
                    # }
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
        workbooks (filter: { projectNameWithin: ["URLS"] } ) {
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

                    ... on CalculatedField {
                        __typename
                        formula
                    }

                    ... on ColumnField {
                        __typename
                        name
                    }
                    
                    upstreamColumns {
                        __typename
                        name
                        table {
                            __typename
                            name
                        }
                    }

                    upstreamTables {
                        __typename
                        name
                    }

                    upstreamDatasources {
                        __typename
                        name                    
                    }

                    datasource {
                        __typename
                        name
                        
                        downstreamDashboards {
                            name
                            path
                            luid
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
            luid
            uri
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
            name
            id
            # fields {
            #     name
            #     fullyQualifiedName
            # }       
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

    DATABASE_TABLES_TO_DASHBOARDS = '''
    query table_fields {
        # databaseTables (filter: {connectionTypeWithin: ["hyper", "webdata-direct", "textscan"]}}) {
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
