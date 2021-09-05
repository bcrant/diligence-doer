import boto3
from helpers import log


def write_to_dynamodb(record, pk):
    """
    Write to DynamoDB

    Args:
        record (dict): A Python Dictionary containing at minimum 'pk' key and value and no keys with Null values.
        pk (str): The field name of the Primary Key value in record
    Returns:
        None

    Using the Resource class (Service Resource & Table)...
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#service-resource
    ...allows us to avoid having to format all writes in the attribute name-value pairs syntax.

    However, the Client class...
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#client
    ...supports all functionalities of the Dynamodb API and may be a better choice for query operations.

    # NOTE: The Table defined in diligence-doer-stack.ts requires the use
    # of a Primary Key (a Partition Key and/or Sort Key). This value must be
    # included in all writes using the following syntax: {'pk': 'string-a-ling-a-ding-dong'}

    """

    session = boto3.Session(profile_name='bc')
    dynamodb = session.resource('dynamodb')
    table = dynamodb.Table(name='diligence-doer')
    # log('key_schema', table.key_schema)

    # Validate input is Python Dictionary
    if isinstance(record, dict):
        if bool(record.get(pk)):
            response = table.put_item(
                Item=record
            )
            return print(response)
        else:
            print(f'[ VALUE ERROR ] Input given to write_to_dynamodb missing Partition Key.'
                  'Missing Partition Key is defined in lib/diligence-doer-stack.ts as: "pk"')

    else:
        print(f'[ TYPE ERROR ] Input given to write_to_dynamodb is of type {type(record)}. Expected input of type: {dict}')
