# ======================================================================================================================
# !/usr/bin/env python3
# Filename: gcloud_nlp.py
# Description: Gcloud related NLP functionalities
# Author: Henin Karkada <henin.roland@gmail.com>
#        # Python Environment - Python3
# Dependencies:
# 1. pip install gcloud
# ======================================================================================================================
# Standard imports
# Imports the Google Cloud client library

from heroka_nlp import logger

try:
    from google.cloud import language
    from google.cloud.language import enums
    from google.cloud.language import types
except ImportError as ie:
    logger.error(ie)

from collections import defaultdict

class GCloudNLP:
    def __init__(self):
        self.client = language.LanguageServiceClient()

    def sentiment(self, text):
        try:

            document = types.Document(content=text,
                                      type=enums.Document.Type.PLAIN_TEXT)

            # Detects the sentiment of the text
            sentiment = self.client.analyze_sentiment(document=document).document_sentiment
            sentiment_result = { 'score': sentiment.score, 'magnitude': sentiment.magnitude }
            return sentiment_result
        except Exception as error:
            logger.error(error)

    def recognise_entities(self,
                             text):
        """
        Functionalities to recognise entities
        :param text: Sentence
        :return:
        """
        try:
            combined_entities = defaultdict(list)
            encoding_type = 'UTF32'
            document = language.types.Document(content=text, type=language.enums.Document.Type.PLAIN_TEXT)
            response = self.client.analyze_entities(document=document, encoding_type=encoding_type)
            for entity in response.entities:
                combined_entities[enums.Entity.Type(entity.type).name].append(entity.name)
            self.entities = dict(combined_entities) 
            return {'combined_entities': dict(combined_entities), 'metadata': response}
            #{response.entity, response.type,response.metadata, response.salience}
        except Exception as error:
            logger.error(error)
