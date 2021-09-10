import pprint
from github import Github
from utils.authentication import authenticate_github
from utils.helpers import *


def get_repo():
    #
    # Query Github REST API
    #

    # Authentication
    PAT, repo_variables = authenticate_github()
    repo_path = str(repo_variables.get('owner') + '/' + repo_variables.get('name'))

    # Get Repository and identify files containing SQL
    repo = Github(PAT).get_repo(repo_path)
    contents = repo.get_contents('')[0:10]
    files_containing_sql = get_files_containing_sql(contents)

    return files_containing_sql


def check_file_extension(file_name):
    split_name = file_name.rsplit('.')

    if len(split_name) >= 2 and split_name[1] in ['sql', 'yml']:
        # print('Contains SQL? True \t\t file: {:60s} \t\t extension: {}'.format(file_name, split_name[1]))
        return True
    else:
        # print('Contains SQL? False \t file: {:60s} \t\t extension: {}'.format(file_name, split_name[1]))
        return False


def get_files_containing_sql(repo_contents):
    files_containing_sql_list = list()

    while repo_contents:
        file_content = repo_contents.pop(0)
        if file_content.type == "dir":
            repo_contents.extend(repo.get_contents(file_content.path))
        elif check_file_extension(file_content.name):
            files_containing_sql_list.append(file_content)
        else:
            # print('File does not contain SQL.')
            continue

    print('Collected all files in repo containing SQL...')
    pprint.pprint(files_containing_sql_list)

    return files_containing_sql_list


if __name__ == "__main__":
    get_repo()
