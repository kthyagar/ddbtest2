import botocore
import boto3
from base64 import b64encode, b64decode


class DDBItem:
    pass

def serialize(obj_in, is_root=False):

    t = type(obj_in)

    if t is dict:
        d = {}
        for key in obj_in:
            ser_d_item = serialize(obj_in[key])
            if ser_d_item:
                d[key] = ser_d_item
        if is_root:
            return d
        else:
            return {"M": d}
    elif t is list:
        l = []
        for item in obj_in:
            ser_l_item = serialize(item)
            if ser_l_item:
                l.append(ser_l_item)
        return {"L": l}
    elif t is DDBItem:
        if is_root:
            return serialize(obj_in.__dict__, is_root=True)
        else:
            return serialize(obj_in.__dict__)
    elif t is int:
        return {'N': str(obj_in)}
    elif t is float:
        return {'N': str(obj_in)}
    elif t is bool:
        return {'BOOL': "true" if obj_in else "false"}
    elif t is str and obj_in:
        return {'S': obj_in}
    elif t is bytes and obj_in:
        return {'B': b64encode(obj_in)}
    elif obj_in:
        raise TypeError("Object is not of the right type for serialization")
    else:
        return None

def getnumber(rawNum):
    try:
        return int(rawNum)
    except ValueError:
        return float(rawNum)

def deserialize(rawitem):

    if type(rawitem) is list:
        itemlist = []
        for item in rawitem:
            itemlist.append(deserialize(item))
        return itemlist

    item = DDBItem()

    for key in rawitem:
        if key == "M":
            return deserialize(rawitem[key])
        elif key == "L":
            items = []
            for list_item in rawitem[key]:
                items.append(deserialize(list_item))
            return items
        elif key == "S":
            return rawitem[key]
        # TODO: handle float
        elif key == "N":
            return getnumber(rawitem[key])
        elif key == "BOOL":
            return rawitem[key] == "true"
        elif key == "B":
            return b64decode(rawitem[key])
        else:
            setattr(item, key, deserialize(rawitem[key]))

    return item

def jdefault(o):
    return o.__dict__


class DDBConn:

    def __init__(self, host=None):
        self._dynamo = boto3.client('dynamodb')
        if host:
            self._dynamo._endpoint.host = host

    def put_item(self, **in_args):

        ser_item = serialize(in_args["Item"], is_root=True)
        in_args["Item"] = ser_item

        if "ExpressionAttributeValues" in in_args:
            ser_attr_values = {}
            for aval in in_args["ExpressionAttributeValues"]:
                key = next(iter(aval.keys()))
                ser_attr_values[key] = serialize(aval[key], is_root=True)

            in_args["ExpressionAttributeValues"] = ser_attr_values

        return self._dynamo.put_item(**in_args)

    def _get_item_with_hash_key(self, table_name, hash_key, consistent_read=False):
        ser_hash_key = serialize(hash_key, is_root=True)
        raw_item = self._dynamo.get_item(TableName=table_name, Key=ser_hash_key, ConsistentRead=consistent_read)
        return deserialize(raw_item["Item"])

    def _get_item_with_hash_and_range_key(self, table_name, hash_key, range_key, consistent_read=False):
        hash_key_attr = next(iter(hash_key.keys()))
        ser_hash_key_val = hash_key[hash_key_attr]
        range_key_attr = next(iter(range_key.keys()))
        ser_range_key_val = range_key[range_key_attr]
        key_set = serialize({hash_key_attr: ser_hash_key_val, range_key_attr: ser_range_key_val}, is_root=True)
        raw_item = self._dynamo.get_item(TableName=table_name, Key=key_set, ConsistentRead=consistent_read)
        return deserialize(raw_item["Item"])

    def get_item(self, table_name, hash_key, range_key=None, consistent_read=False):
        if range_key:
            return self._get_item_with_hash_and_range_key(table_name, hash_key, range_key, consistent_read)
        else:
            return self._get_item_with_hash_key(table_name, hash_key, consistent_read)

    def query(self, **in_args):

        ser_attr_values = {}
        for aval in in_args["ExpressionAttributeValues"]:
            key = next(iter(aval.keys()))
            ser_attr_values[key] = serialize(aval[key], is_root=True)

        in_args["ExpressionAttributeValues"] = ser_attr_values

        raw_items = self._dynamo.query(**in_args)

        return deserialize(raw_items["Items"])

    def increment_counter(self, counter_name, counter_table_name="counter"):

        counter = self.get_item(table_name=counter_table_name,
                                hash_key={"counter_name": counter_name},
                                consistent_read=True)

        counter.val += 1

        put_complete = False

        while not put_complete:
            try:
                self.put_item(TableName=counter_table_name,
                              Item=counter,
                              ConditionExpression="counter_name <> :pid OR (counter_name = :pid AND val <= :v)",
                              ExpressionAttributeValues=[{":pid": counter_name}, {":v": counter.val-1}])
                put_complete = True
                # log
            except botocore.exceptions.ClientError as ex:
                if ex.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    put_complete = False
                    counter.val += 1
                else:
                    raise

        return counter.val

    def create_table(self, **args):
        return self._dynamo.create_table(**args)

    def delete_table(self, **args):
        return self._dynamo.delete_table(**args)

    def list_tables(self, **args):
        ret = self._dynamo.list_tables(**args)
        return ret["TableNames"]
