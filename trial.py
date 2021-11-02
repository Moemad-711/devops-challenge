from seqtolang import Detector
from transformers import pipeline

# Initialize seqtolang Language detector
detector = Detector()

# Initialize sentiment analysis classifier 
classifier = pipeline('sentiment-analysis')

# Initialize translator
#fr_to_en_translator = pipeline("translation_fa_to_en")
#de_to_en_translator = pipeline("translation_de_to_en")
translator = pipeline(task='text2text-generation', model='facebook/m2m100_418M')

if __name__ == "__main__":
    langs = detector.detect('Hello my friends! How are you doing today?')
    print(langs)
    print(langs[0][0])
    print(translator('Hello my friends! How are you doing today?', forced_bos_token_id = translator.tokenizer.get_lang_id(lang='de'))[0]['generated_text'])

    print(classifier('Hello my friends! How are you doing today?'))