import json
import decimal
import boto3
from seqtolang import Detector
from transformers import pipeline


# Initialize boto3 clients & resources
dynamoDB= boto3.resource('dynamodb')
postprocessed_table = dynamoDB.Table('postprocessed_reviews')

# Create a Custom Decimal Encoder class
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)

# Initialize sentiment analysis classifier 
classifier = pipeline('sentiment-analysis')


def handler(event, context):
    '''
    This is a lambda fucntion used to process the new movie reviews. It is triggered by the S3 bucket.

    '''
    # Logging the trigger event
    print(str(event))

    # Iterating on Items
    for item in event:
        # Logging Current Item Key 
        print('iterating on item with key: ' + item['_id'])
        
        original_review_text = item['review_text']

        if item['detected_review_lang'] == 'eng':

            # Performing sentiment analysis
            sentiment_analysis_tokens = classifier(original_review_text)
            sentiment_score = sentiment_analysis_tokens[0]['score']
            sentiment_class = sentiment_analysis_tokens[0]['label']
            
            # Logging Sentiment Analysis Operation
            print('Finised Sentiment Analysis')
            
            # Adding new information to the review dict  
            item['sentiment_score'] = sentiment_score
            item['sentiment_class'] = sentiment_class

            # Logging Processed Item 
            print('Processed Review: ' + str(item))

            # Inserting the new processed review to the Database
            postprocessed_table.put_item(Item=item)
            
            # Logging database insert operation
            print('Inserted an item into the database')
        
        else:
            
            # Translating French Text inro English
            translated_review_text = item['translated_review_text']

            # Performing sentiment analysis
            sentiment_analysis_tokens = classifier(translated_review_text)
            sentiment_score = sentiment_analysis_tokens[0]['score']
            sentiment_class = sentiment_analysis_tokens[0]['label']
            
            # Logging Sentiment Analysis Operation
            print('Finised Sentiment Analysis')
            
            # Adding new information to the review dict  
            item['sentiment_score'] = sentiment_score
            item['sentiment_class'] = sentiment_class

            # Logging Processed Item 
            print('Processed Review: ' + str(item))

            # Inserting the new processed review to the Database
            postprocessed_table.put_item(Item=item)
            
            # Logging database insert operation
            print('Inserted an item into the database')
            

    return None