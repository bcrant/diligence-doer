import os
import tableauserverclient as TSC
from dotenv import load_dotenv
load_dotenv()


def authenticate_tableau():
    #
    # Tableau Authentication
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


def authenticate_github():
    #
    # Github GraphQL API Authentication
    #
    GITHUB_PAT = os.getenv('GITHUB_PAT')
    GITHUB_REPO_OWNER = os.getenv('GITHUB_REPO_OWNER')
    GITHUB_REPO_NAME = os.getenv('GITHUB_REPO_NAME')

    headers = {
        'Authorization': str('token' + ' ' + GITHUB_PAT)
    }

    repo_info = {
        'owner': GITHUB_REPO_OWNER,
        'name': GITHUB_REPO_NAME
    }

    return headers, repo_info
