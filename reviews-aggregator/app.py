import json
import boto3
import decimal

# Initialize boto3 clients and resources
s3_client = boto3.client('s3')
lambda_client = boto3.client('lambda')
dynamoDB = boto3.resource('dynamodb')
preprocessed_table = dynamoDB.Table('preprocessed_reviews')

# Create a Custom Decimal Encoder class
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def handler(event, context):
    '''
    This is a lambda fucntion used to store the new movie reviews in a DynamoDB and 
    trigger the processing lambda fucntion if the reviews count surpassed a certain threshold. 
    It is triggered by the S3 bucket.
    
    Input: 
        Event: describe the event trigger from the S3 bucket and includes the JSON file uploaded to the bucket.

    Return: 
        None
    '''
    # Logging the trigger event
    print(str(event))

    # Setting the Threshold
    threshold = 5

    # Getting the new JSON file's name and the S3 bucket's name
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    json_file_name = event['Records'][0]['s3']['object']['key']


    # Getting the JSON file content from the new file in the S3 Bucket
    json_file_object = s3_client.get_object(Bucket=bucket_name, Key=json_file_name)
    json_file_reader = json_file_object['Body'].read()
    json_file_dict = json.loads(json_file_reader)

    # Logging
    print('Converted the review into a Dict.')


    # Checking the number of items in the DynamoDB
    dynamodb_response = preprocessed_table.scan(Select='ALL_ATTRIBUTES')
    table_items = dynamodb_response['Items']
    
    # Logging Scan Result
    print('Scanned the temp table for items and found ' + str(len(table_items)))

    if len(table_items) < threshold-1:
        
        # Putting the new json review to DynamoDB instance 
        preprocessed_table.put_item(Item=json_file_dict)

        # Logging
        print(' Put new review to the temp table')
        
        return None

    elif len(table_items) == threshold - 1:

        # Adding the last item to the array
        table_items.append(json_file_dict)
        
        # Creating  a JSON String
        json_table_items = json.dumps(table_items, cls=DecimalEncoder)
        
        # Logging items to be processed
        print('collected ' + str(threshold) + ' Items')
        print(json_table_items)
        
        # Removing Items from the DynamoDB Table
        for item in table_items:
            print('Deleting Item with key: ' + str(item['_id']))
            delete_response = preprocessed_table.delete_item(Key={'_id': item['_id']})

        # Invoke the review processing lambda function 
        lambda_response = lambda_client.invoke(FunctionName='processing_reviews_trial',
                                                InvocationType='Event', 
                                                Payload=json_table_items.encode())
        
        return None


    return None
