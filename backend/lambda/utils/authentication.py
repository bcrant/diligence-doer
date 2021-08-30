import os
import tableauserverclient as TSC
from dotenv import load_dotenv
load_dotenv()


def authenticate_tableau():
    #
    # AUTHENTICATION
    #
    TOKEN_NAME = os.getenv('TABLEAU_PAT_NAME')
    TOKEN = os.getenv('TABLEAU_PAT')
    SERVER_URL = os.getenv('TABLEAU_SERVER_URL')

    auth = TSC.PersonalAccessTokenAuth(
        token_name=TOKEN_NAME,
        personal_access_token=TOKEN
    )

    server = TSC.Server(
        server_address=SERVER_URL,
        use_server_version=True
    )

    return auth, server
