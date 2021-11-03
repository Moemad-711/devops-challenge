import json
import decimal
import boto3
from transformers import pipeline


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

# Initialize translator
translator = pipeline(task='text2text-generation', model='facebook/m2m100_418M')


def handler(event, context):
    '''
    This is a lambda fucntion used to translate review text if needed and trigger the sentimetn analyzer text.

    Input: 
        Event: describe the event trigger from the S3 bucket and includes the JSON Objects with the detected original review text language.

    Return: 
        None
    '''
    # Logging the trigger event
    print(str(event))

    # Iterating on Items
    for item in event:
        # Logging Current Item Key 
        print('iterating on item with key: ' + item['_id'])

        original_review_text = item['review_text']


        if item['detected_review_lang'] == 'eng':
            
            translated_review_text = ''

            # Adding new information to the review dict  
            item['translated_review_text'] = translated_review_text


        else:
            
            # Translating French Text inro English
            translated_review_text = translator(original_review_text, 
                                                forced_bos_token_id=translator.
                                                                    tokenizer
                                                                    .get_lang_id(lang='en'))

            # Adding new information to the review dict  
            item['translated_review_text'] = translated_review_text[0]['generated_text']

            # Logging Processed Item 
            print('translated Review text: ' + translated_review_text[0]['generated_text'])

    # Invoke Sentiment Analyzer Lmabda function
    items = json.dumps(event,cls=DecimalEncoder)
    lambda_reponse = lambda_client.invoke(FunctionName='analyze-review-text',
                                            InvocationType='Event', 
                                            Payload=items.encode())


    return None