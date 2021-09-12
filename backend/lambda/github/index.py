from get_repository import get_repo


def handler(event):
    """
    Query the Github REST API
    """

    get_repo()

    return
