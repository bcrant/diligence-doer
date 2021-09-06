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

        database_tables = SERVER.metadata.query(MetadataQueries.DATABASE_TABLES)
        pp(database_tables['data'])

        # custom_sql = SERVER.metadata.query(MetadataQueries.CUSTOM_SQL_TABLES)
        # pp(custom_sql['data'])

        # published_datasources = SERVER.metadata.query(MetadataQueries.PUBLISHED_DATASOURCES)
        # pp(published_datasources['data'])

        # embedded_datasources = SERVER.metadata.query(MetadataQueries.EMBEDDED_DATASOURCES)
        # pp(embedded_datasources['data'])


if __name__ == "__main__":
    get_metadata()
