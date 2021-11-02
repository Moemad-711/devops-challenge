import json
import decimal
import boto3
from seqtolang import Detector

# Initialize boto3 clients & resources
lambda_client = boto3.client('lambda')
dynamoDB= boto3.resource('dynamodb')
postprocessed_table = dynamoDB.Table('postprocessed_reviews')

# Create a Custom Decimal Encoder class
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

# Initialize seqtolang Language detector
detector = Detector()



def handler(event, context):
    '''
    This is a lambda fucntion used to detect languages of the new movie reviews and then invoke the lambda function responsible for translation.

    '''
    # Logging the trigger event
    print(str(event))

    # Iterating on Items
    for item in event:
        # Logging Current Item Key 
        print('iterating on item with key: ' + item['_id'])
        
        # original text language detection
        original_review_text = item['review_text']
        review_lang = detector.detect(original_review_text)
        detected_review_lang = review_lang[0][0]
        
        # Adding detected language to the dict
        item['detected_review_lang'] = detected_review_lang

        # Logging review lang
        print('oringinal review lang: ' + str(detected_review_lang))

    items = json.dumps(event,cls=DecimalEncoder)

    lambda_reponse = lambda_client.invoke(FunctionName='translate-review-text',
                                            InvocationType='Event', 
                                            Payload=items.encode())


    return None