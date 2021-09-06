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
