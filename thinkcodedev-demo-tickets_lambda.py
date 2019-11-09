import boto3
import json
import uuid


print_verbose = True
databaseTable = "thinkcodedev-demo-tickets_tickets"


def lambda_handler(event, context):
    print_to_log("INFO", "RECEIVED EVENT", json.dumps(event, indent=2), print_verbose)
    bucketname = get_bucketname_from_event(event)
    filename = get_filename_from_event(event)
    data = get_data_from_filename(bucketname, filename)
    write_data_to_database_table(data, databaseTable)
    return True


def print_to_log(type, subject, message, print_verbose=False):
    verbose_types = ("error", "err", "fail")
    if not print_verbose and type.lower() not in verbose_types:
        return False
    print("[" + type + "] " + subject)
    print(message)
    return True


def get_bucketname_from_event(event):
    bucketname = event['Records'][0]['s3']['bucket']['name']
    print_to_log("INFO", "RETRIEVED BUCKET NAME FROM EVENT", bucketname, print_verbose)
    return bucketname


def get_filename_from_event(event):
    filename = event['Records'][0]['s3']['object']['key']
    print_to_log("INFO", "RETRIEVED FILENAME FROM EVENT", filename, print_verbose)
    return filename


def get_data_from_filename(bucketname, filename):
    s3_client = boto3.client('s3')
    json_object = s3_client.get_object(Bucket=bucketname, Key=filename)
    json_contents = json.loads(json_object['Body'].read())
    print_to_log("INFO", "RETRIEVED DATA FROM FILE", json_contents, print_verbose)
    return json_contents


def write_data_to_database_table(data, table_name):
    dynamo_client = boto3.resource('dynamodb')
    table = dynamo_client.Table(table_name)
    uuid = get_random_uuid()
    data['ticket_id'] = uuid
    table.put_item(Item=data)
    print_to_log("INFO", "DATA WRITTEN TO DATABASE USING RANDOM UUID", uuid, print_verbose)
    return True


def get_random_uuid():
    return str(uuid.uuid4())
