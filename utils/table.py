import botocore.exceptions

from config import AWS_RESOURCE_NAME


def create_table(table_name, session):
    try:
        dynamodb_client = session.client(AWS_RESOURCE_NAME)
        return dynamodb_client.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'Username',
                    'KeyType': 'HASH'
                },
                {
                    "AttributeName": "OrderId",
                    "KeyType": "RANGE"
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'Username',
                    'AttributeType': 'S'
                },
                {
                    "AttributeName": "OrderId",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "Amount",
                    "AttributeType": "N"
                }
            ],
            LocalSecondaryIndexes=[
                {
                    "IndexName": "UserAmountIndex",
                    "KeySchema": [
                        {
                            "AttributeName": "Username",
                            "KeyType": "HASH"
                        },
                        {
                            "AttributeName": "Amount",
                            "KeyType": "RANGE"
                        }
                    ],
                    "Projection": {
                        "ProjectionType": "KEYS_ONLY"
                    }
                }

            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            },
        )
    except botocore.exceptions.ClientError as e:
        return f"ClientError: {e}"
    except Exception as e:
        print('Aborting ..............')
        print(e)


def get_table(table_name, session):
    client = session.client(AWS_RESOURCE_NAME)
    try:
        return client.describe_table(
            TableName=table_name
        )
    except botocore.exceptions.ClientError as e:
        return f"ClientError: {e}"
    except Exception as e:
        print('Aborting ..............')
        print(e)


def delete_table(table_name, session):
    client = session.client(AWS_RESOURCE_NAME)
    try:
        return client.delete_table(
            TableName=table_name,
        )
    except botocore.exceptions.ClientError as e:
        return f"ClientError: {e}"
    except Exception as e:
        print('Aborting ..............')
        print(e)


def update_table_add_gsi(table_name, session):
    client = session.client(AWS_RESOURCE_NAME)
    try:
        return client.update_table(
            TableName=table_name,
            AttributeDefinitions=[
                {
                    "AttributeName": "ReturnDate",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "OrderId",
                    "AttributeType": "S"
                }
            ],
            GlobalSecondaryIndexUpdates=[
                {
                    "Create": {
                        "IndexName": "ReturnDateOrderIdIndex",
                        "KeySchema": [
                            {
                                "AttributeName": "ReturnDate",
                                "KeyType": "HASH"
                            },
                            {
                                "AttributeName": "OrderId",
                                "KeyType": "RANGE"
                            }
                        ],
                        "Projection": {
                            "ProjectionType": "ALL"
                        },
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 1,
                            "WriteCapacityUnits": 1
                        }
                    }
                }
            ]
        )
    except botocore.exceptions.ClientError as e:
        return f"ClientError: {e}"
    except Exception as e:
        print('Aborting ..............')
        print(e)
