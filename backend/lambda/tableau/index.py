from get_tableau_metadata import get_metadata


def handler(event, context):
    """
    Query the Tableau Metadata API
    """

    get_metadata()

    return
