import boto3
from decimal import Decimal
import numpy as np
from boto3.dynamodb.conditions import Key, Attr


def get_table_metadata(table):
    """
    Get some metadata about chosen table.
    """

    return {
        'num_items': table.item_count,
        'primary_key_name': table.key_schema[0],
        'status': table.table_status,
        'bytes_size': table.table_size_bytes,
        'global_secondary_indices': table.global_secondary_indexes
    }


def scan_table(table, filtering_exp=None):
    """
    Scan a table using a filter. filtering_exp (optional)
    must be of the format Key(<item>)<comparison operator>(<value>)

    Sample usage:
        Item_list = scan_table(table, Key('runtime').lt(9))['Items']
    You can also chain the filters with & (and), | (or), ~ (not), like so:
        list = scan_table(table, Key('runtime').lt(9) & Key('runtime').lt(9))
    """
    if filtering_exp is not None:
        response = table.scan(FilterExpression=filtering_exp)
    else:
        response = table.scan()

    return response


def query_table(table, filtering_exp=None):
    """
    Very similar to scan(). You need to use a key with
    query, unlike with scan.
    """
    if filtering_exp is not None:
        response = table.query(KeyConditionExpression=filtering_exp)
    else:
        response = table.query()

    return response



def read_table_item(table, primary_key, pk_value, secondary_key=None, sk_value=None):
    """
    Return item read by primary key.
    """
    if secondary_key is not None:
        response = table.get_item(Key={primary_key: pk_value,
                               secondary_key: sk_value})
    else:
        response = table.get_item(Key={primary_key: pk_value})

    return response



def add_Item(table, item):
    """
    Add an item to the table. You must include the
    keys in the item object.
    """
    response = table.put_item(Item=item)

    return response


def update_Item(table, keys, itm, value):
    """
    Update an item in the table. Add the keys object first, then
    the item object (itm), then it's new value.
    """
    table.update_item(
        Key=keys,
        UpdateExpression="SET #attr = :Val",
        ExpressionAttributeNames={'#attr': itm},
        ExpressionAttributeValues={':Val': value}
    )


def update_SubAttribute(table, keys, attr, sub_attr, value):
    """
    If an item's attribute has multiple sub-attributes within, you can
    update these at one level deep with this function.
    """
    table.update_item(
        Key=keys,
        UpdateExpression="SET #itm.#sub_itm = :Val",
        ExpressionAttributeNames={
          '#itm': attr,
          '#sub_itm': sub_attr
        },
        ExpressionAttributeValues={
          ':Val': value
        },
    )


def delete_Item(table, primary_key, pk_value, secondary_key=None, sk_value=None):
    """
    Delete a table item using its primary key.
    """
    if secondary_key is not None:
        table.delete_item(Key={primary_key: pk_value,
                               secondary_key: sk_value})
    else:
        table.delete_item(Key={primary_key: pk_value})
