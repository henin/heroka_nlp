# Standard imports
# Imports the Google Cloud client library
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types

class GCloud():
    def __init__(self):
        pass
        #self.client = ""

class GCloudNLP():
    def __init__(self):
        self.client = language.LanguageServiceClient()

    def sentiment(self, text):

        document = types.Document(content=text,
            type=enums.Document.Type.PLAIN_TEXT)

        # Detects the sentiment of the text
        sentiment = self.client.analyze_sentiment(document=document).document_sentiment
        sentiment_result = { 'score': sentiment.score, 'magnitude': sentiment.magnitude }
        return sentiment_result

    def entities_recognition(self,
                   text):
            encoding_type='UTF32'
            document = language.types.Document(content=text, type=language.enums.Document.Type.PLAIN_TEXT)
            response = self.client.analyze_entities(document=document, encoding_type=encoding_type)
            return response
        #{response.entity, response.type,response.metadata, response.salience}

def main():
    pass
    #obj = NLP()
    #   response = obj.entity_recognition("Henin", "UTF32")
    #   return response

if __name__ == '__main__':
    main()
