try:
    import boto3
    from boto3.s3.transfer import S3Transfer
    from boto3.exceptions import *
except ImportError as ie:
    logger.error(ie)
    sys.exit(1)

queue_name = "gstConnection"
region_name = "ap-southeast-1"



def get_credentials(aws_keys=None):
    """
    Get credentials details from the json
    :return: credentials
    """
    try:
        with open(CREDENTIALS_INFO, 'r') as fp:
            credentials = json.load(fp)
    except IOError as ie:
        logger.error(
            "Credentials.json file not found in path: `{}`. Please keep the config files in specified folder"
            .format(CREDENTIALS_INFO))
        sys.exit(1)

    except Exception as error:
        logger.error(error)

    return credentials



def get_s3_buckets_details(bucket_name=None):
    """
    Get the bucket level details
    :param bucket_name:
    :return:
    """
    logger.info("Into `{}`() . .".format(whoami()))
    s3 = get_service('s3')

    if not bucket_name:
        for bucket in s3.buckets.all():
            print(("\n Buckets present:", bucket.name))
    elif bucket_name:
        for bucket in s3.buckets.all():
            if bucket.name.startswith(bucket_name):
                print(("\n Buckets present:", bucket.name))

def get_service(service_name,
                service_type=None,
                service_region_name=None,
                aws_access_key_id=None,
                aws_secret_access_key=None):
    """
    Returns the AWS service details when provided the service name
    :param service_name:
    :return:
    """

    credentials = get_credentials()
    if not service_region_name:
        service_region_name = region_name
    try:
        # @TODO: To be removed
        if aws_access_key_id and aws_secret_access_key:
            AWS_ACCESS_KEY_ID = aws_access_key_id
            AWS_ACCESS_KEY_SECRET = aws_secret_access_key
        else:
            AWS_ACCESS_KEY_ID = credentials['AWS_ACCESS_KEY_ID']
            AWS_ACCESS_KEY_SECRET = credentials['AWS_ACCESS_KEY_SECRET']
        credentials = {
            'aws_access_key_id': AWS_ACCESS_KEY_ID,
            'aws_secret_access_key': AWS_ACCESS_KEY_SECRET
        }
    except KeyError as key_error:
        logger.error(key_error)

    if service_type == 'client':
        return boto3.client(service_name, service_region_name, **credentials)
    else:
        return boto3.resource(service_name, service_region_name, **credentials)

def delete_data_sqs(sqs, queue_name, messages_to_delete):
    """
    Delete queue data one at a time
    :param sqs: sqs
    :param queue_name: Queue  name
    :param messages_to_delete: Messages to be delete with receipt handle and ID
    :return: None
    """
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        if len(messages_to_delete) == 0:
            logger.info("Queue processed successfully")
        else:
            delete_response = queue.delete_messages(Entries=messages_to_delete)
            if delete_response.get('Successful', [{}]) and messages_to_delete:
                delete_response.get('Successful', [{}])[0]['Id'] == \
                messages_to_delete[0]['Id']
                logger.info("Successfully Deleted Message ID: `{}` "
                            "from `{}`".format(messages_to_delete[0]['Id'],
                                               queue_name))

    except Exception as error:
        logger.error(error)

def clear_sqs_queue(sqs,
                    queue_name,
                    number_of_messages=1,
                    wait_time=6,
                    attributes_name=None):
    """
    Clearing SQS queue, based on an attribute or all messages
    :param sqs:
    :param attributes:
    :return: queue_res
    """
    logger.info("Into `{}`() . .".format(whoami()))
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        # Clear all messages

        if queue:
            queue_res = [message.delete() for message in \
                         queue.receive_messages(
                             VisibilityTimeout=1,
                             MaxNumberOfMessages=number_of_messages,
                             WaitTimeSeconds=wait_time)]
    except Exception as error:
        time.sleep(1)
        queue_res = [
            message.delete() for message in queue.receive_messages(
                VisibilityTimeout=1,
                MaxNumberOfMessages=number_of_messages,
                WaitTimeSeconds=wait_time)
        ]
        logger.error(error)

    if not queue_res:
        logger.info("SQS queue has been cleared for {}".format(queue_name))

    return queue_res

def get_data_sqs(sqs, queue_name, number_of_messages=1, wait_time=6):
    """
    Get queue data from the AWS SQS
    :param sqs: SQS client
    :param queue_name: Queue name
    :param number_of_messages: Number of messages to be returned
    :param wait_time: Wait time if messages are not returned
    :return:
    """
    logger.info("Into `{}`() to get `{}` data from `{}` queue  . .".format(
        whoami(), number_of_messages, queue_name))
    message_bodies = []
    messages_to_delete = []
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)

        if not queue:
            time.sleep(1)
            queue = sqs.get_queue_by_name(QueueName=queue_name)
        else:
            message = queue.receive_messages(
                VisibilityTimeout=1,
                MaxNumberOfMessages=number_of_messages,
                WaitTimeSeconds=wait_time)
            if not message:
                time.sleep(1)
                message = queue.receive_messages(
                    VisibilityTimeout=1,
                    MaxNumberOfMessages=number_of_messages,
                    WaitTimeSeconds=wait_time)

            body = json.loads(message[0].body)
            message_bodies.append(body)
            messages_to_delete.append({
                'Id':
                message[0].message_id,
                'ReceiptHandle':
                message[0].receipt_handle
            })
            return message_bodies, messages_to_delete

    except Exception as error:
        time.sleep(1)
        try:
            queue = sqs.get_queue_by_name(QueueName=queue_name)
            if not queue:
                time.sleep(1)
                queue = sqs.get_queue_by_name(QueueName=queue_name)
            else:
                message = queue.receive_messages(
                    VisibilityTimeout=1,
                    MaxNumberOfMessages=number_of_messages,
                    WaitTimeSeconds=wait_time)
                if not message:
                    time.sleep(1)
                    message = queue.receive_messages(
                        VisibilityTimeout=1,
                        MaxNumberOfMessages=number_of_messages,
                        WaitTimeSeconds=wait_time)
                else:
                    return [], []
                body = json.loads(message[0].body)
                message_bodies.append(body)
                messages_to_delete.append({
                    'Id':
                    message[0].message_id,
                    'ReceiptHandle':
                    message[0].receipt_handle
                })
        except Exception as error:
            return [], []
            logger.error(error)

    return message_bodies, messages_to_delete

def clear_queue_populate_data(queue_name,
                              payload,
                              number_of_messages=1,
                              wait_time=6):
    """
    Clear queue and populate data for the Test queue, used for
    debugging/testing purposes
    :param queue_name: Queue Name
    :param payload: Queue data
    :param number_of_messages: Default 1
    :param wait_time: Default 6
    :return: None
    """
    try:
        sqs = get_service('sqs')
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        queue_res = clear_sqs_queue(
            sqs,
            queue_name,
            number_of_messages=number_of_messages,
            wait_time=wait_time)
        while True:
            queue_res = clear_sqs_queue(
                sqs,
                queue_name,
                number_of_messages=number_of_messages,
                wait_time=wait_time)
            if not queue_res:
                break
        send_data_sqs(sqs, queue_name, payload, number_of_messages, wait_time)

    except Exception as error:
        logger.error(error)

def send_data_sqs(sqs, queue_name, data, number_of_messages=1, wait_time=6):
    """
    Send Data in JSON/text to SQS queue
    :param sqs:
    :param queue_name:
    :param data:
    :return: None
    """
    logger.info("Into `{}`() . .".format(whoami()))
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        if queue:
            queue.send_message(MessageBody=json.dumps(data))
            logger.info("Data `{}` sent successfully to the queue {}".format(
                data, queue_name))
    except Exception as error:
        time.sleep(1)
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        if queue:
            queue.send_message(MessageBody=json.dumps(data))
        else:
            logger.error(error)
            return

def check_data_sqs(sqs, queue_name, number_of_messages=1, wait_time=6):
    """
    Check Data in JSON/text to SQS queue
    :param sqs: SQS client
    :param queue_name: Queue name
    :param number_of_messages: No of messages to be returned
    :param wait_time: Wait time if no messages to be retrieved
    :return: messages
    """

    messages = set()
    try:
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        for i in range(0, number_of_messages):
            msg_list = queue.receive_messages(
                VisibilityTimeout=1,
                MaxNumberOfMessages=number_of_messages,
                WaitTimeSeconds=wait_time)
            for msg in msg_list:
                messages.add(msg.body)

    except Exception as error:
        time.sleep(1)
        logger.error(error)
        try:
            queue = sqs.get_queue_by_name(QueueName=queue_name)
            for i in range(0, number_of_messages):
                msg_list = queue.receive_messages(
                    VisibilityTimeout=1,
                    MaxNumberOfMessages=number_of_messages,
                    WaitTimeSeconds=wait_time)
                for msg in msg_list:
                    messages.add(msg.body)

        except Exception as error:
            logger.error(error)
            exit(1)
    return messages

def upload_s3(args):
    """
    Upload Objects to S3 buckets to a particular bucket using botoi, Uploads as
    a public URL
    :param args: contains filename, s3_filename, bucket, region
    :return:
    """
    logger.info("Uploading file `{}` in s3 bucket: `{}`".format(
        args[1], args[2]))
    credentials = get_credentials()
    AWS_ACCESS_KEY_ID = credentials.get('AWS_ACCESS_KEY_ID')
    AWS_ACCESS_KEY_SECRET = credentials.get('AWS_ACCESS_KEY_SECRET')
    credentials = {
        'aws_access_key_id': AWS_ACCESS_KEY_ID,
        'aws_secret_access_key': AWS_ACCESS_KEY_SECRET
    }

    if len(args) > 0 and len(args) < 4:
        filename = args[0]
        s3_filename = args[1]
        s3_bucket = args[2]

    if len(args) == 4:
        filename = args[0]
        s3_filename = args[1]
        s3_bucket = args[2]

        region = args[3]
    else:
        region = 'ap-southeast-1'
    try:
        s3 = get_service('s3')
        client = boto3.client('s3', region, **credentials)
        transfer = S3Transfer(client)
        # bucket = s3.Bucket("testbank-nc")
        # bucket.upload_file("C:\Users\henin\Downloads\captcha-600.png", "captcha2.png")
        transfer.upload_file(
            filename,
            s3_bucket,
            s3_filename,
            extra_args={'ACL': 'public-read'})
    except Exception as error:
        logger.error(error)
    # except botocore.exceptions.EndpointConnectionError as error:
    #     logger.error(error)

def get_s3_file(s3, bucket_name, filename):
    """
    Get s3 object
    :param s3:
    :param bucket_name:
    :param filename:
    :return: S3 obj
    """
    try:
        obj = s3.Object('testbank-nc', 'captcha.png')
    except Exception as error:
        logger.error(error)
    return obj

def delete_bucket_s3(client, bucket_name, region):
    """
    Delete a specific bucket
    :param client:
    :param bucket_name:
    :param region:
    :return:
    """
    try:
        response = client.delete_bucket(Bucket=bucket_name)
    except Exception as error:
        logger.error(error)

    return response

def delete_s3_objects(bucket_name, region, objects):
    """
    Deletes a specific or list of objects
    :param client:
    :param bucket_name:
    :param region:
    :return:
    """
    client = get_service('s3', 'client')
    try:
        if isinstance(objects, list):
            response = client.delete_objects(
                Delete={
                    'Objects': [
                        {
                            'Key': objects,
                            'VersionId': 'string'
                        },
                    ],
                    'Quiet': True | False
                },
                MFA='string',
                RequestPayer='requester')
        else:
            client.delete_object(Bucket=bucket_name, Key=objects)
        logger.info("Deleted `{}` object from {}".format(objects, bucket_name))
    except Exception as error:
        logger.error(error)

def list_buckets_s3(client, bucket_name, region):
    """
    List buckets
    :param s3:
    :param bucket_name:
    :param region:
    :return:
    """
    response = client.list_buckets()
    return

def create_bucket_s3(s3, bucket_name, region):
    """
    Create Bucket in s3
    :param s3:
    :param bucket_name:
    :param region:
    :return: response
    """
    try:
        response = s3.create_bucket(
            ACL='private' | 'public-read' | 'public-read-write'
            | 'authenticated-read',
            Bucket=bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint':
                'EU' | 'eu-west-1' | 'us-west-1' | 'us-west-2' | 'ap-south-1'
                | 'ap-southeast-1' | 'ap-southeast-2' | 'ap-northeast-1'
                | 'sa-east-1' | 'cn-north-1' | 'eu-central-1'
            },
            GrantFullControl='string',
            GrantRead='string',
            GrantReadACP='string',
            GrantWrite='string',
            GrantWriteACP='string')
        return response
    except Exception as error:
        logger.error(error)

def create_presigned_url(bucket_name, object_name, region, expiration=3600):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param region: string

    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    credentials = get_credentials()
    s3_client = boto3.client('s3', config= boto3.session.Config(signature_version='s3v4'),aws_access_key_id=credentials['AWS_ACCESS_KEY_ID'], aws_secret_access_key=credentials['AWS_ACCESS_KEY_SECRET'], region_name = region)
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                        'Key': object_name},
                                                    ExpiresIn=expiration)
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL
    return response

def download_file_s3_bucket_local(bucket_name,
                                  s3_file_path,
                                  local_filename,
                                  region_name=None):
    """
    Downloads file from s3 bucket to local
    :param bucket_name: S3 Bucket name
    :param file_path:
    :return: s3_file_path = "users_{}/headerless_files/{}".format(user_id,
    s3_filename)
    """
    try:
        s3 = get_service('s3', 'client', region_name)
        s3.download_file(bucket_name, s3_file_path, local_filename)
        time.sleep(2)
    except botocore.exceptions.ClientError as e:
        logger.error("The object {} does not exist. {}".format(
            e, s3_file_path))
        warning_mesage = "The object {} does not exist. {}".format(e, s3_file_path)
        return warning_mesage


def copy_files_bucket_to_bucket(src_bucket_name, dest_bucket_name,
                                src_object_name, dest_object_name,
                                region_name):
    '''
    copy files from one s3 bucket to other s3 bucket

    :param src_bucket_name: string
    :param src_object_name: string
    :param dest_bucket_name: string. Must already exist.
    :param dest_object_name: string(path to where the object should copy). If dest bucket/object exists, it is
    overwritten. Default: src_object_name
    :return: True if object was copied, otherwise False
    '''

    try:
        copy_source = {'Bucket': src_bucket_name, 'Key': src_object_name}
        if dest_object_name is None:
            dest_object_name = src_object_name

        # Copy the object
        s3 = get_service('s3', 'client', region_name)

        try:
            s3.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket_name,
                Key=dest_object_name)
        except ClientError as e:
            logger.error(e)
            return False
        return True
    except Exception as error:
        logger.error(error)

def create_table_dynamodb(dynamodb,
                          table_name,
                          keyschema,
                          attribute_definitions=None,
                          provisioned_throughput=None):
    try:
        #create_table_queryy = """eval(keyschema)

        table = dynamodb.create_table(
            TableName=table_name,
            KeySchema=eval(keyschema),
            AttributeDefinitions=eval(attribute_definitions),
            ProvisionedThroughput=eval(provisioned_throughput))

        #The below parameters are sent to the function

        #keyschema = """[{'AttributeName': 'analysis_id',
        #                 'KeyType': 'HASH',  #Partition key
        #                }]"""
        #attribute_definitions = """[{'AttributeName': 'analysis_id',
        #                             'AttributeType': 'N'
        #                        }]"""
        #provisioned_throughput = """{'ReadCapacityUnits': 1,
        #                             'WriteCapacityUnits': 1
        #                         }"""

        #table = dynamodb.create_table(TableName=table_name,   KeySchema=[
        #     {
        #         'AttributeName': attribute_name,
        #         'KeyType': key_type  #Partition key
        #     }],
        #     AttributeDefinitions=[
        #     {
        #         'AttributeName': 'id',
        #         'AttributeType': 'N'
        #     }],
        #     ProvisionedThroughput={
        #     'ReadCapacityUnits': 1,
        #     'WriteCapacityUnits': 1
        #})
        return table
    except Exception as error:
        logger.error(error)

def delete_dynamodb_data(dynamodb,
                         table_name,
                         keys=None,
                         partition_key=None,
                         partition_type=None,
                         search_key=None,
                         search_value=None):
    try:

        table = dynamodb.Table(table_name)
        if not partition_key:
            partition_key = 'analysis_id'
            partition_type = 'int'

        if isinstance(keys, dict):
            table.delete_item(Key=keys)
            logger.info("Deleting item: `{}` from dynamodb table: `{}`".format(
                table_name, keys))

        elif keys == 'all' and not search_key:
            table_items = get_data_dynamodb(dynamodb, table_name)
            if partition_key and partition_type:
                partition_ids = [
                    eval(partition_type)(item[partition_key])
                    for item in get_data_dynamodb(dynamodb, table_name)
                    if item[partition_key]
                ]
            else:
                partition_ids = [
                    item[partition_key]
                    for item in get_data_dynamodb(dynamodb, table_name)
                    if item[partition_key]
                ]
            if partition_ids:
                [
                    table.delete_item(Key={partition_key: id})
                    for id in partition_ids
                ]
                logger.info("Deleted items from `{}` table".format(table))
            else:
                print("No items to delete")
        elif keys == 'all' and partition_key and search_value and search_key and partition_type:
            partition_ids = [
                item[partition_key] for item in get_data_dynamodb(
                    dynamodb,
                    table_name,
                    fieldname=search_key,
                    fieldvalue=search_value,
                    scan_type=True)
            ]
            if partition_ids:
                [
                    table.delete_item(Key={partition_key: id})
                    for id in partition_ids
                ]
                print("Deleted all items from `{}` table".format(table))
            else:
                print("No items to delete")
        elif keys == 'drop':
            logger.warning("Are you sure you want to delete the table: {}".
                           format(table_name))
            logger.warning("Dropping dynamodb table: {}".format(table_name))
            table.delete()

    except Exception as error:
        error_msg = "Error: {} while deleting dynamodb items".format(error)
        logger.error(error_msg)

def get_data_dynamodb(dynamodb,
                      table_name,
                      num_data=-1,
                      fieldname=False,
                      fieldvalue=False,
                      scan_type=False):
    """
    Get a particular item from dynamodb using scan
    :param dynamodb: dynamodb object
    :param table_name: Table name
    :param num_data: If -1 is specified gets all data, or N data
    :param fieldname: To scan the fieldname
    :param fieldvalue: fieldvalue to scan
    :param scan_type: scan_type if true with fieldname and value will get the results
    """
    try:
        table = dynamodb.Table(table_name)
        if scan_type and fieldname and fieldvalue:
            response = table.scan(
                FilterExpression=Attr(fieldname).eq(fieldvalue))
        elif fieldname and fieldvalue:
            response = dynamodb.get_item(
                TableName=table_name, Key={fieldname: fieldvalue})
        else:
            response = table.scan()

        if response and num_data == -1:
            item = response['Items']
            return item
        elif response and num_data >= 1:
            item = response['Items']
            return item[:num_data]

    except Exception as error:
        logger.error(error)

def insert_data_dynamodb(dynamodb, table_name, json_data):
    """
    Insert a json into dynamodb
    :param table_name: Name of the Table
    :param json_data: JSON object
    """
    try:
        table = dynamodb.Table(table_name)
        table.put_item(Item=json_data)
        logger.info("Inserted JSON data: {} into table: `{}`".format(
            json_data, table_name))
    except Exception as error:
        logger.error(error)

def update_data_dynamodb(dynamodb, table_name, search_key_fields,
                         update_key_fields):
    """
    Update Json value in dynamodb
    :param dynamodb: Dynamodb object
    :param table_name: Table name
    :param fieldname: Field name
    :param fieldvalue: field value
    """
    try:

        update_fields = [
            '{} = :val{}'.format(v, i + 1)
            for i, v in enumerate(update_key_fields.keys())
        ]
        update_fields = 'SET ' + ','.join(update_fields)
        expression_attribute_values = [
            "':val{}': '{}'".format(i + 1, v)
            for i, v in enumerate(update_key_fields.values())
        ]
        expression_attribute_values = '{' + ','.join(
            expression_attribute_values) + '}'
        expression_attribute_values = eval(expression_attribute_values)
        table = dynamodb.Table(table_name)
        table.update_item(
            Key=search_key_fields,
            UpdateExpression=update_fields,
            ExpressionAttributeValues=expression_attribute_values)

    except Exception as error:
        logger.error(error)





def list_named_query_athena(client, next_token, max_results=123):
    """
    List all named queries in Athena
    :param client: Boto Athena Client
    :param next_token:
    :param max_results:
    :return: response
    """

    logger.info("Into `{}`() . .".format(whoami()))
    try:
        response = client.list_named_queries(
            NextToken=next_token, MaxResults=max_results)
        return response
    except Exception as error:
        logger.error(error)
    return response

def execute_athena_query(client,
                         database,
                         query,
                         s3_output,
                         query_results=False):
    """
    Execute athena SQL like queries
    :param client: Boto Client Athena
    :param database: Database name
    :param query: SQL query
    :param s3_output: s3 Output Location
    :param query_results: If query_results are true, then return the output
    of SQL command
    :return: response
    """
    logger.info("Into `{}`() . .".format(whoami()))
    try:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={
                'OutputLocation': s3_output,
            })
        time.sleep(1)
        if not response:
            time.sleep(1)
            response = client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': database},
                ResultConfiguration={
                    'OutputLocation': s3_output,
                })
        if response and query_results:
            return_response = return_query_results_athena(
                client, response['QueryExecutionId'].encode('utf-8'))
            return return_response
            if not return_response:
                time.sleep(1)
                return_response = return_query_results_athena(
                    client, response['QueryExecutionId'].encode('utf-8'))
                return return_response
        else:
            return response

    except Exception as error:
        logger.error(error)
        exit(1)

def return_query_results_athena(client, query_id, max_results=123):
    """
    Returns the results of a single query execution specified by query but
    returns results.
    :param client: Boto Athena Client
    :param query_id: Query ID of the query
    :param next_token:
    :param max_results:
    :return:
    """
    logger.info("Into `{}`() . .".format(whoami()))
    try:
        response = client.get_query_results(QueryExecutionId=query_id)
        if not response:
            time.sleep(1)
            response = client.get_query_results(QueryExecutionId=query_id)

        if response.get('ResultSet', '').get('Rows'):
            response = response['ResultSet']['Rows']
        else:
            response = {}

    except Exception as error:
        logger.warning("Unable to get Query result on first hit!!! "
                       "Retrying....")
        time.sleep(1)
        try:
            response = client.get_query_results(QueryExecutionId=query_id)
            time.sleep(1)
            if response:
                response = response['ResultSet']['Rows']
            else:
                response = {}

        except Exception as error:
            logger.error("Unable to get Query result: {}!!! "
                         "Exiting!!!....".format(error))
            exit(1)
    return response

def execute_named_query(client, database, query_name, query, unique_id):
    """
    Execute named query
    :param client: Boto Athena Client
    :param database: Database name
    :param query_name: Named query identifier
    :param query: SQL query
    :param unique_id: Here it is user_docid , CreateNamedQuery request is
    received,
    the same response is returned and another query is not created
    :return: response
    """
    logger.info("Into `{}`() . .".format(whoami()))
    try:
        response = client.create_named_query(
            Name='string',
            Description='string',
            Database='string',
            QueryString='string',
            ClientRequestToken='string')
    except Exception as error:
        logger.error(error)
    return response


def upload_s3_transfer(filename, s3_filename, bucket_name, region_name):
    try:
        logger.info("Uploading the document : {} in S3".format(filename))
        s3_transfer = get_service(
            's3', service_type='client', service_region_name=region_name)
        transfer = S3Transfer(s3_transfer)
        transfer.upload_file(filename, bucket_name, s3_filename)
        return filename
    except Exception as error:
        logger.error(error)
        warning_mesage = "File not Uploaded"
        return warning_mesage

def upload_s3_transfer2(filename, s3_filename, bucket_name, region_name):
    try:
        logger.info("Uploading the document : {} in S3".format(filename))
        s3_transfer = get_service(
            's3', service_type='client', service_region_name=region_name)
        #transfer = S3Transfer(s3_transfer)
        s3_transfer.upload_file(filename, bucket_name, s3_filename)
        #transfer.upload_file(filename, bucket_name, s3_filename)
        return filename
    except Exception as error:
        logger.error(error)

def s3_upload(filename, s3_filename, bucket_name, region):
    try:
        client_data = get_client_info('bankstatement')

        host = client_data['nc_db_hostname']
        read_host = client_data['nc_db_read_hostname']
        username = client_data['nc_db_username']
        password = client_data['nc_db_password']
        database = client_data['nc_database']
        project_path = client_data['project_path']
        charset = "utf8mb4"
        cursorclass = pymysql.cursors.DictCursor
        AWS_ACCESS_KEY_ID = client_data['AWS_ACCESS_KEY_ID']
        AWS_ACCESS_KEY_SECRET = client_data['AWS_ACCESS_KEY_SECRET']
        conn = boto.s3.connect_to_region(
            region,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_ACCESS_KEY_SECRET)
        bucket = conn.get_bucket(bucket_name)
        k = boto.s3.key.Key(bucket)
        k.key = s3_filename  #"users_"+str(user_id)+"/"+str(filename)
        k.set_contents_from_filename(str(filename))
        return str(filename)
    except Exception as error:
        logger.error(error)


def get_list_files_s3_path(bucket_name,
                           region_name,
                           prefix=None,
                           aws_access_key_id=None,
                           aws_secret_access_key=None,
                           extensions=None):
    try:
        filenames = []
        if aws_access_key_id and aws_secret_access_key:
            s3 = get_service(
                "s3",
                service_region_name=region_name,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key)
        else:
            s3 = tools.get_service("s3", service_region_name=region_name)
        bucket = s3.Bucket(bucket_name)

        if prefix:
            bucket_objs = bucket.objects.filter(Prefix=prefix)
        else:
            bucket_objs = bucket.objects.filter()

        for file in bucket_objs:
            try:
                if extensions and os.path.basename(
                        file.key).split('.')[1] in extensions:
                    filenames.append(file.key)
                elif not extensions:
                    filenames.append(file.key)
            except:
                continue
        return filenames
    except Exception as error:
        logger.error(error)

def upload_inputs(local_files_dir, user_id, s3_bucket, region_name, aws_access_key_id=None, aws_secret_access_key=None):
    """
    Testing function to upload list of files from your local folder to S3 bucket
    Usage: tools.upload_payloads('pdfs/', 2017, 'testbank-nc', 'ap-southeast-1')
           tools.upload_payloads('pdfs/', 100,'AKxxxxxxxxxxxxxxxxxxTQ','Xxxxxxxxxxysnkfgvjkbkdjfbkxxxxx', 'ncfinbank', 'ap-south-1')
    :param local_files_dir: directory containing files
    :param user_id: numeric value i.e the folder path for users_{}
    :param aws_access_key_id: aws access key
    :param aws_secret_access_key: aws secret key
    :param s3_bucket: s3 bucket name
    :param region_name: aws region
    :return:
    """
    if aws_access_key_id and aws_secret_access_key:
         s3 = get_service("s3", service_type='client',
                 service_region_name=region_name,
                 aws_access_key_id=aws_access_key_id,
                 aws_secret_access_key=aws_secret_access_key)
    else:
        s3 = get_service("s3", service_type='client', service_region_name=region_name)

    pdfs_list = glob.glob(local_files_dir + '/*')
    if not pdfs_list:
        print("No files to upload")
        return

    for pdf in pdfs_list:
        try:
            if user_id:
                s3_file_path = "users_{}/{}".format(user_id,
                                                    os.path.basename(pdf))

                transfer = S3Transfer(s3)
                logger.info(
                    "Uploading file {} to path {} in bucket {} of region {}".
                    format(
                        os.path.basename(pdf), s3_file_path, s3_bucket,
                        region_name))
                transfer.upload_file(pdf, s3_bucket, s3_file_path)
                logger.info("Uploaded successfully ")

        except Exception as error:
            logger.error(error)

def download_output(folder_path, user_id, s3_bucket, region_name, aws_access_key_id=None, aws_secret_access_key=None):
    """
    Testing function to download a specific file from S3 bucket to your local folder
    Usage: tools.download_output('1432.pdf', 2017,'testbank-nc','ap-southeast-1')
           tools.download_output('263.pdf', 100,'AKIAJBSQLXXXXXXXXXX', 'PpaC4xxxxxxxaL6Txxxxxxxxxxxxxxkc', 'ncfinbank','ap-south-1')
    :param file: filename which is in S3
    :param user_id: numeric value i.e the folder path for users_{}
    :param aws_access_key_id: aws access key
    :param aws_secret_access_key: aws secret key
    :param s3_bucket: s3 bucket name
    :param region_name: aws region
    :return:
    """
    if aws_access_key_id and aws_secret_access_key:
        s3 = get_service("s3", service_type='client',
                 service_region_name=region_name,
                 aws_access_key_id=aws_access_key_id,
                 aws_secret_access_key=aws_secret_access_key)

    else:
        s3 = get_service("s3", service_type='client', service_region_name=region_name)
    filenames = get_list_files_s3_path(s3_bucket,
                                         region_name,
                                         prefix="users_{}".format(user_id),
                                         aws_access_key_id=aws_access_key_id,
                                         aws_secret_access_key=aws_secret_access_key,
                                         extensions=None)

    try:
        folder_path = "{}/users_{}_{}".format(folder_path, user_id, datetime.now().strftime('%Y%m%d%H%M%S%f'))
        os.makedirs(folder_path)
        for s3_file_path in filenames:

            local_file = '{}/{}'.format(folder_path, os.path.basename(s3_file_path))
            transfer = S3Transfer(s3)
            logger.info(
                "Downloading file to {} from path {} of bucket {} in region {} "
                .format(local_file, s3_file_path, s3_bucket, region_name))
            transfer.download_file(s3_bucket, s3_file_path, local_file)
            logger.info("Downloaded sucessfully ")
    except Exception as error:
        logger.error(error)



def read_file_from_s3(s3_bucket, s3_file_path):
    """
    reading the body of a file directly from S3
    :param s3_bucket: s3 bucket where the file is stored
    :param s3_file_path: absolute path of the file to read
    :return:
    """
    try:
        s3 = get_service('s3')
        obj = s3.Object(s3_bucket, s3_file_path)
        body = obj.get()['Body'].read().decode("utf-8")
        return body
    except Exception as error:
        logger.error(error)
