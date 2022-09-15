import boto3
from pprint import pprint
from boto3.dynamodb.conditions import Attr, Key

from config import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME, TABLE_NAME, AWS_RESOURCE_NAME
from utils.crud import insert_item, get_item, scan_table, update_item, delete_item, bulk_insert_item, bulk_get_item, \
    batch_update, bulk_delete_item
from utils.table import create_table, get_table, delete_table, update_table_add_gsi

session = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION_NAME
)

# CREATE TABLE
# table = create_table(TABLE_NAME, session)
# print(table)

# GET TABLE METADATA
# table_meta_data = get_table(TABLE_NAME, session)
# pprint(table_meta_data)


# DELETE TABLE
# result = delete_table(TABLE_NAME, session)
# print(result)


# INSERT ITEM
# item = {
#     'Username': 'Parth971',
#     'OrderId': 'OrderId3',
#     'ReturnDate': 'ReturnDate1'
# }
# result = insert_item(TABLE_NAME, session, item)
# print(result)


# GET ITEM
# key = {
#     'Username': 'Parth971'
# }
# result = get_item(TABLE_NAME, session, key)
# print(result)



# UPDATE ITEM
# key = {
#     'Username': 'Parth971'
# }
# expression_attribute_name = {
#     "#qa": "QA",
# }
# expression_attribute_value = {
#     ':v': 'QA-2'
#
# }
# update_expression = "SET #qa = :v"
#
# result = update_item(
#         TABLE_NAME,
#         session,
#         key,
#         expression_attribute_name,
#         expression_attribute_value,
#         update_expression
# )
# print(result)


# DELETE ITEM
# key = {
#     'id': 1,
#     'name': 'Parth Desai'
# }
# result = delete_item(TABLE_NAME, session, key)
# print(result)


# BULK WRITE ITEM
# content = [
#     {
#         'Username': f'User{i}',
#         'OrderId': f'Order100{i}'
#     } for i in range(10)
# ]
# result = bulk_insert_item(TABLE_NAME, session, content)
# print(result)


# BATCH GET ITEMS
# content = {
#     TABLE_NAME: {
#         'Keys': [
#             {
#                 'Username': 'User0'
#             },
#             {
#                 'Username': 'User1'
#             },
#         ],
#         'ConsistentRead': True
#     }
# }
# result = bulk_get_item(session, content)
# print(result)


# BATCH DELETE ITEMS
# content = [
#     {
#         'Username': f'User{i}',
#         'OrderId': f'Order100{i}'
#     } for i in range(10)
# ]
# result = bulk_delete_item(TABLE_NAME, session, content)
# print(result)


# BATCH UPDATE
# content = [
#     [
#         {
#             'Username': f'User{i}',
#             'OrderId': f'Order{i}'
#         },
#         {
#             "#a": "Amount",
#         },
#         {
#             ':v': i
#         },
#         "SET #a = :v"
#     ] for i in range(10)
# ]
# batch_update(TABLE_NAME, session, content)


# Adding GSI | UPDATING TABLE
# result = update_table_add_gsi(TABLE_NAME, session)
# print(result)


# QUERYING
dynamodb = session.resource(AWS_RESOURCE_NAME)
table = dynamodb.Table(TABLE_NAME)

# result = table.query(
#     KeyConditionExpression=Key('Username').eq('User3') & Key('OrderId').eq('Order3'),
#     ReturnConsumedCapacity='TOTAL',
#     ConsistentRead=True
# )
# pprint(result)


# result = table.query(
#     KeyConditionExpression=Key('Username').eq('User3') & Key('Amount').eq(0),
#     ReturnConsumedCapacity='TOTAL',
#     ConsistentRead=True,
#     IndexName='UserAmountIndex'
# )
# pprint(result)


# result = table.query(
#     KeyConditionExpression=Key('Username').eq('User3'),
#     FilterExpression=Attr('Amount').gt(0),
#     ReturnConsumedCapacity='TOTAL',
#     ConsistentRead=True,
# )
# pprint(result)


# result = table.query(
#     KeyConditionExpression=Key('Username').eq('User3') & Key('Amount').gte(3),
#     ReturnConsumedCapacity='TOTAL',
#     ConsistentRead=True,
#     IndexName='UserAmountIndex'
# )
# pprint(result)

# result = table.scan(
#     IndexName='ReturnDateOrderIdIndex'
# )
# pprint(result)

# result = table.query(
#     KeyConditionExpression=Key('ReturnDate').eq('ReturnDate1'),
#     ReturnConsumedCapacity='TOTAL',
#     IndexName='ReturnDateOrderIdIndex'
# )
# pprint(result)


# result = table.query(
#     KeyConditionExpression=Key('Username').eq('Parth971'),
#     ReturnConsumedCapacity='TOTAL',
# )
# pprint(result)


result = table.query(
    KeyConditionExpression=Key('ReturnDate').eq('ReturnDate1') & Key('OrderId').eq('OrderId2'),
    # FilterExpression=Attr('ReturnDate').eq('ReturnDate1'),
    ReturnConsumedCapacity='TOTAL',
    IndexName='ReturnDateOrderIdIndex'
)
pprint(result)


# SCAN TABLE TO GET ALL ROWS
# result = scan_table(TABLE_NAME, session)
# pprint(result)
