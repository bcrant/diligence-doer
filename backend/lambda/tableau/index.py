from get_tableau_metadata import get_metadata


def handler(event):
    """
    Query the Tableau Metadata API
    """
    get_metadata()

    return
