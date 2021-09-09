import requests
from utils.authentication import authenticate_github
from utils.helpers import *
from utils.queries import GithubQueries


def get_repo_files():
    #
    # Query Github GraphQL API
    #

    # Authentication
    headers, repo_variables = authenticate_github()

    # Parameters
    url = 'https://api.github.com/graphql'

    # Query Github GraphQL API
    response = requests.post(
        url,
        headers=headers,
        json={"query": GithubQueries.REPO_FILES, "variables": repo_variables}
    ).json()

    repo_files = response.get('data').get('repository').get('object').get('entries')
    # repo_files = response
    #
    # pp(repo_files)
    #
    # get_nested_file_ids = get_files_recursively_from_roots(repo_files)

    get_nested_children(url, headers)

    # for oid in get_nested_file_ids:
    #     print('Querying individual object...', oid)
    #     get_nested_children(url, headers)


def get_files_recursively_from_roots(root_files):
    nested_ids_list = []

    for f in root_files:
        if f.get('type') == "tree":
            nested_ids_list.append(f.get('oid'))

    return nested_ids_list


def get_nested_children(api_url, auth):

    gid = {"git_obj": "MDEwOlJlcG9zaXRvcnk0MDQ1MzkyODU="}

    # Query Github GraphQL API
    response = requests.post(
        api_url,
        headers=auth,
        json={"query": GithubQueries.NESTED_FILES, "variables": gid}
    ).json()

    pp(response)


if __name__ == "__main__":
    get_repo_files()
