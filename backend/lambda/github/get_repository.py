from github import Github
from parse_yml import parse_yml
from utils.authentication import authenticate_github
from utils.dynamodb import write_to_dynamodb
from utils.helpers import *


def get_repo():
    #
    # Query Github REST API
    #

    # Authentication
    endpoint, token, repo_path = authenticate_github()

    # Adjust endpoint based on whether a personal or Enterprise Github Account is in use
    if not endpoint:
        github_repo = Github(login_or_token=token).get_repo(repo_path)
    else:
        github_repo = Github(base_url=endpoint, login_or_token=token).get_repo(repo_path)

    # Get Repository and identify files containing SQL
    repo_files_list = get_files_recursively(github_repo)
    yml_files, sql_files = get_files_containing_sql(repo_files_list)

    map_file_path_to_url = dict((yml_file.path, yml_file.html_url) for yml_file in yml_files)

    mapped_tables_to_files = parse_yml(yml_files, map_file_path_to_url)

    for table_name, file_dict in mapped_tables_to_files.items():
        map_dict = {
            'pk': table_name,
            'github': {
                'files': {**file_dict}
            }
        }

        write_to_dynamodb(
            record=map_dict,
            pk='pk',
            sk='github'
        )

    return


def get_files_recursively(repo):
    print(f'Inspecting all files in Github repository: {repo.full_name}...')
    repo_contents = repo.get_contents('')[8:9]

    repo_files_list = list()

    while repo_contents:
        file_content = repo_contents.pop(0)

        # Pop files out of directories to iterate over all files in repo recursively
        if file_content.type == "dir":
            print('{:64s} {}'.format('Found directory:', file_content.name))
            print('Inspecting all files in directory...')
            repo_contents.extend(repo.get_contents(file_content.path))

        else:
            print('{:64s} {}'.format('Found file:', file_content.name))
            repo_files_list.append(file_content)

    return repo_files_list


def get_files_containing_sql(repo_files):
    print(f'Collecting all files containing SQL...')
    sql_files_list = list()
    yml_files_list = list()

    for repo_file in repo_files:
        # Find all files in repo containing SQL (Currently only supporting SQL and YML)
        split_name = repo_file.name.rsplit('.')
        # if len(split_name) >= 2 and split_name[1] in ['sql', 'yml']:
        if len(split_name) >= 2 and split_name[-1] == 'yml':
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

    return yml_files_list, sql_files_list


def get_file_extension(file_name):
    split_name = file_name.rsplit('.')

    if len(split_name) >= 2 and split_name[1] in ['sql', 'yml']:
        # print('Contains SQL? True \t\t file: {:60s} \t\t extension: {}'.format(file_name, split_name[1]))
        return True
    else:
        # print('Contains SQL? False \t file: {:60s} \t\t extension: {}'.format(file_name, split_name[1]))
        return False


if __name__ == "__main__":
    get_repo()
