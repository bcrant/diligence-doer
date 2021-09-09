import requests
from utils.authentication import authenticate_github
from utils.helpers import *


def get_repo_files():
    #
    # Query Github GraphQL API
    #

    # Authentication
    headers, repo_info = authenticate_github()

    # Parameters
    url = 'https://api.github.com/graphql'

    query = '''
    query repoFiles($owner: String!, $name: String!) {
        repository(owner: $owner, name: $name) {
            object(expression: "HEAD:") {
                ... on Tree {
                    entries {
                        name
                        type
                        mode
            
                        object {
                        ... on Blob {
                            byteSize
                            text
                            isBinary
                            }
                        }
                    }
                }
            }
        }
        variables {
            owner: ''' + repo_info.get('owner') + ''',
            name: ''' + repo_info.get('name') + '''
        }
    }
    '''

    print(query)

    # Query Github GraphQL API
    response = requests.post(
        url,
        headers=headers,
        json={"query": query}
    ).json()

    pp(response)


if __name__ == "__main__":
    get_repo_files()
