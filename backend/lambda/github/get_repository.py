import pprint
from github import Github
from utils.authentication import authenticate_github
from utils.helpers import *


def get_repo():
    #
    # Query Github REST API
    #

    # Authentication
    endpoint, token, repo_path = authenticate_github()

    # Get Repository and identify files containing SQL
    github_repo = Github(base_url=endpoint, login_or_token=token).get_repo(repo_path)
    repo_files_list = get_files_recursively(github_repo)
    yml_files, sql_files = get_files_containing_sql(repo_files_list)

    # TODO: Either use the .decoded_contents or .download_url to parse files

    return yml_files, sql_files


def get_files_recursively(repo):
    print(f'Inspecting all files in Github repository: {repo.full_name}...')

    repo_contents = repo.get_contents('')

    repo_files_list = list()

    while repo_contents:
        file_content = repo_contents.pop(0)

        # Pop files out of directories to iterate over all files in repo recursively
        if file_content.type == "dir":
            repo_contents.extend(repo.get_contents(file_content.path))

        else:
            repo_files_list.append(file_content)

    return repo_files_list


def get_files_containing_sql(repo_files):
    print(f'Collecting all files containing SQL...')
    sql_files_list = list()
    yml_files_list = list()

    for repo_file in repo_files:
        # Find all files in repo containing SQL (Currently only supporting SQL and YML)
        split_name = repo_file.name.rsplit('.')
        if len(split_name) >= 2 and split_name[1] in ['sql', 'yml']:
            if split_name[1] == 'sql':
                sql_files_list.append(repo_file)
            elif split_name[1] == 'yml':
                yml_files_list.append(repo_file)
            else:
                # print(f'File: {repo_file.name} does not contain SQL.')
                continue
        else:
            # print('File: {repo_file.name} is a system dotfile and will not be parsed for SQL.')
            continue

    log('yml files', yml_files_list)
    log('sql files', sql_files_list)

    return yml_files_list, sql_files_list


def get_file_extension(file_name):
    split_name = file_name.rsplit('.')

    if len(split_name) >= 2 and split_name[1] in ['sql', 'yml']:
        # print('Contains SQL? True \t\t file: {:60s} \t\t extension: {}'.format(file_name, split_name[1]))
        return True
    else:
        # print('Contains SQL? False \t file: {:60s} \t\t extension: {}'.format(file_name, split_name[1]))
        return False


# def map_extension_to_file(file, extension):


if __name__ == "__main__":
    get_repo()
