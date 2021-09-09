from github import Github
from utils.authentication import authenticate_github
from utils.helpers import *


def get_repo():
    #
    # Query Github GraphQL API
    #

    # Authentication
    PAT, repo_variables = authenticate_github()

    g = Github(PAT)

    log('github', g)


if __name__ == "__main__":
    get_repo()
