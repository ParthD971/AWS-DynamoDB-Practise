from config import AWS_RESOURCE_NAME
import botocore.exceptions


def insert_item(table_name, session, item):
    dynamodb = session.resource(AWS_RESOURCE_NAME)
    table = dynamodb.Table(table_name)
    try:
        return table.put_item(
            Item=item
        )
    except botocore.exceptions.ClientError as e:
        return f"ClientError: {e}"
    except Exception as e:
        print('Aborting..........')
        print(e)


def get_item(table_name, session, key):
    dynamodb = session.resource(AWS_RESOURCE_NAME)
    table = dynamodb.Table(table_name)
    try:
        return table.get_item(
            Key=key,
            # ProjectionExpression="Age, Username" # for retrieving specific fields
        )
    except botocore.exceptions.ClientError as e:
        return f"ClientError: {e}"
    except Exception as e:
        print('Aborting..........')
        print(e)


def scan_table(table_name, session):
    dynamodb = session.resource(AWS_RESOURCE_NAME)
    table = dynamodb.Table(table_name)
    try:
        return table.scan()
    except botocore.exceptions.ClientError as e:
        return f"ClientError: {e}"
    except Exception as e:
        print('Aborting..........')
        print(e)


def update_item(
        table_name,
        session,
        key,
        expression_attribute_name,
        expression_attribute_value,
        update_expression
):
    dynamodb = session.resource(AWS_RESOURCE_NAME)
    table = dynamodb.Table(table_name)
    try:
        return table.update_item(
            Key=key,
            ExpressionAttributeNames=expression_attribute_name,
            ExpressionAttributeValues=expression_attribute_value,
            UpdateExpression=update_expression,
        )
    except botocore.exceptions.ClientError as e:
        return f"ClientError: {e}"
    except Exception as e:
        print('Aborting..........')
        print(e)


def delete_item(table_name, session, key):
    dynamodb = session.resource(AWS_RESOURCE_NAME)
    table = dynamodb.Table(table_name)
    try:
        return table.delete_item(
            Key=key
        )
    except botocore.exceptions.ClientError as e:
        return f"ClientError: {e}"
    except Exception as e:
        print('Aborting..........')
        print(e)


def bulk_insert_item(table_name, session, content):
    try:
        dynamodb = session.resource(AWS_RESOURCE_NAME)
        table = dynamodb.Table(table_name)
        with table.batch_writer() as batch:
            for obj in content:
                batch.put_item(Item=obj)
    except botocore.exceptions.ClientError as e:
        return f"ClientError: {e}"
    except Exception as e:
        print('Aborting..........')
        print(e)


def bulk_delete_item(table_name, session, content):
    try:
        dynamodb = session.resource(AWS_RESOURCE_NAME)
        table = dynamodb.Table(table_name)
        with table.batch_writer() as batch:
            for obj in content:
                batch.delete_item(Key=obj)
    except botocore.exceptions.ClientError as e:
        return f"ClientError: {e}"
    except Exception as e:
        print('Aborting..........')
        print(e)


def bulk_get_item(session, content):
    try:
        dynamodb = session.resource(AWS_RESOURCE_NAME)
        return dynamodb.batch_get_item(
            RequestItems=content,
            ReturnConsumedCapacity='TOTAL'
        )
    except botocore.exceptions.ClientError as e:
        return f"ClientError: {e}"
    except Exception as e:
        print('Aborting..........')
        print(e)


def query_item(table_name, session):
    try:
        dynamodb = session.resource(AWS_RESOURCE_NAME)
        table = dynamodb.Table(table_name)
        return table.query(
            KeyConditionExpression={
                'Username': 'User1'
            }
        )
        
    except botocore.exceptions.ClientError as e:
        return f"ClientError: {e}"
    except Exception as e:
        print('Aborting..........')
        print(e)


def batch_update(table_name, session, content):
    for key, ean, eav, ue in content:
        result = update_item(
            table_name,
            session,
            key,
            ean,
            eav,
            ue
        )
        print(result)