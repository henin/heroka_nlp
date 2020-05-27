# ======================================================================================================================
# !/usr/bin/env python3
# Filename: allen_nlp.py
# Description: Allen NLP related functionalities
# Author: Henin Karkada <henin.roland@gmail.com>
#        # Python Environment - Python3

# Dependencies:
# 1. pip install allennlp
# @TODO: Currently failing. Change to Module call rather than API call
# ======================================================================================================================

from collections import defaultdict
import requests
import time
import sys

from heroka_nlp import logger


class AllenNLP:
    # Load English tokenizer, tagger, parser, NER and word vectors
    def __init__(self):
        pass

    def recognise_entities(self,
                           listofnames,
                           entities=None):
        """
        Recoginise entities through Allen NLP API
        :param listofnames: List of sentence tokens
        :param entities: specifiy what entities to return (ORG, PERSON)
        :return:
        """
        names_entity = defaultdict(list)
        for name in listofnames:
            try:
                #Api Approcach
                #url = "https://demo.allennlp.org/predict/named-entity-recognition"
                url = "https://demo.allennlp.org/api/named-entity-recognition/predict"
                #defining a params dict for the parameters to be sent to the API
                payload = {"sentence": name}
                # sending get request and saving the response as response object
                try:
                    post_response = requests.post(url=url, json=payload)
                except:
                    time.sleep(2)
                    post_response = requests.post(url=url, json=payload)
                if post_response.status_code != 200:
                    logger.error("Unable to hit ALLEN NLP API!!!!")
                    sys.exit()

                results = post_response.json()
                if results:
                    names_entity[name] = \
                        {res for res in zip(results['tags'], results['words']) if not res[0] == 'O'}
            except Exception as error:
                logger.warning(error)
                continue

        if entities:
            requested_entities = {}
            for entity in entities:
                requested_entities[entity] = names_entity[entity]
            return dict(requested_entities)
        else:
            return dict(names_entity)
