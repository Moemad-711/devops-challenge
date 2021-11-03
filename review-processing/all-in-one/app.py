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

# Initialize seqtolang Language detector
detector = Detector()

# Initialize sentiment analysis classifier 
classifier = pipeline('sentiment-analysis')

# Initialize translator
translator = pipeline(task='text2text-generation', model='facebook/m2m100_418M')


def handler(event, context):
    '''
    This is a lambda fucntion used to detect the language of the reviews' text, 
    translate it if it's not in english, 
    run sentiment analysis on the english text and 
    then store all review inforamtion in the database.
    '''
    # Logging the trigger event
    print(str(event))

    # Iterating on Items
    for item in event:
        # Logging Current Item Key 
        print('iterating on item with key: ' + item['_id'])
        
        original_review_text = item['review_text']
        review_lang = detector.detect(original_review_text)
        detected_review_lang = review_lang[0][0]
        
        # Logging review lang
        print('oringinal review lang: ' + str(detected_review_lang))

        if detected_review_lang == 'eng':
            translated_review_text = ''

            # Performing sentiment analysis
            sentiment_analysis_tokens = classifier(original_review_text)
            sentiment_score = sentiment_analysis_tokens[0]['score']
            sentiment_class = sentiment_analysis_tokens[0]['label']
            
            # Logging Sentiment Analysis Operation
            print('Finised Sentiment Analysis')
            
            # Adding new information to the review dict  
            item['detected_review_lang'] = detected_review_lang
            item['translated_review_text'] = translated_review_text
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
            translated_review_text = translator(original_review_text, 
                                                forced_bos_token_id=translator.
                                                                    tokenizer
                                                                    .get_lang_id(lang='en'))

            # Performing sentiment analysis
            sentiment_analysis_tokens = classifier(translated_review_text)
            sentiment_score = sentiment_analysis_tokens[0]['score']
            sentiment_class = sentiment_analysis_tokens[0]['label']
            
            # Logging Sentiment Analysis Operation
            print('Finised Sentiment Analysis')
            
            # Adding new information to the review dict  
            item['detected_review_lang'] = detected_review_lang
            item['translated_review_text'] = translated_review_text
            item['sentiment_score'] = sentiment_score
            item['sentiment_class'] = sentiment_class

            # Logging Processed Item 
            print('Processed Review: ' + str(item))

            # Inserting the new processed review to the Database
            postprocessed_table.put_item(Item=item)
            
            # Logging database insert operation
            print('Inserted an item into the database')
            


    return None