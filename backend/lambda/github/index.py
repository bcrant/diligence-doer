from get_repository import get_repo


def handler(event, context):
    """
    Query the Github REST API
    """

    get_repo()

    return
