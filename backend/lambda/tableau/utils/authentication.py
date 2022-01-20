import os
import boto3
import tableauserverclient as TSC
from dotenv import load_dotenv
load_dotenv()


class Environment:
    DEV = 'dev'
    PROD = 'prod'


def authenticate_dynamodb():
    if os.getenv('AWS_EXECUTION_ENV') is None:
        #
        # FOR LOCAL
        #
        session = boto3.Session(profile_name=os.getenv('AWS_PROFILE'), region_name=os.getenv('AWS_REGION_NAME'))
        dynamodb = session.resource('dynamodb')

    else:
        #
        # FOR REMOTE
        #
        dynamodb = boto3.resource('dynamodb')

    return dynamodb


def authenticate_github():
    """
    #
    # Github REST API Authentication
    #

    NOTE: For Github Enterprise with custom hostname, use this url instead:
    url = 'https://{hostname}/api/v3'
    """

    GITHUB_PAT = os.getenv('GITHUB_PAT')
    GITHUB_REPO_OWNER = os.getenv('GITHUB_REPO_OWNER')
    GITHUB_REPO_NAME = os.getenv('GITHUB_REPO_NAME')

    # API Endpoint
    url = None

    repo_path = str(GITHUB_REPO_OWNER + '/' + GITHUB_REPO_NAME)

    return url, GITHUB_PAT, repo_path


def authenticate_github_graphql():
    """
    #
    # Github GraphQL API Authentication
    #
    NOTE: For Github Enterprise with custom hostname, use this url instead:
    url = 'https://{hostname}/api/v3/graphql'

    See: https://docs.github.com/en/graphql/guides/managing-enterprise-accounts
    """
    GITHUB_PAT = os.getenv('GITHUB_PAT')
    GITHUB_REPO_OWNER = os.getenv('GITHUB_REPO_OWNER')
    GITHUB_REPO_NAME = os.getenv('GITHUB_REPO_NAME')

    url = 'https://api.github.com/graphql'

    headers = {
        'Authorization': str('token' + ' ' + GITHUB_PAT)
    }

    repo_info = {
        'owner': GITHUB_REPO_OWNER,
        'name': GITHUB_REPO_NAME
    }

    return url, headers, repo_info


def authenticate_tableau(env):
    #
    # Tableau Authentication
    #
    if env not in (Environment.DEV, Environment.PROD):
        print('Wrong environment supplied to authenticate_tableau function. Expected "dev" or "prod"')

    TOKEN_NAME = os.getenv('TABLEAU_PAT_NAME')
    TOKEN = None
    SERVER_URL = None

    if env == 'dev':
        TOKEN = os.getenv('TABLEAU_DEV_PAT')
        SERVER_URL = os.getenv('TABLEAU_DEV_SERVER_URL')

    if env == 'prod':
        TOKEN = os.getenv('TABLEAU_PROD_PAT')
        SERVER_URL = os.getenv('TABLEAU_PROD_SERVER_URL')

    auth = TSC.PersonalAccessTokenAuth(
        token_name=TOKEN_NAME,
        personal_access_token=TOKEN
    )

    server = TSC.Server(
        server_address=SERVER_URL,
        use_server_version=True
    )

    return auth, server
