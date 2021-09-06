import pprint
from utils.authentication import authenticate_tableau
from utils.helpers import *


def get_metadata():

    # Tableau Authentication
    AUTHENTICATION, SERVER = authenticate_tableau()

    # Log in to Tableau Server and query the Metadata API
    with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):
        all_workbooks, pagination_item = SERVER.workbooks.get()
        print('There are {} workbooks on site...'.format(pagination_item.total_available))


if __name__ == "__main__":
    get_metadata()
