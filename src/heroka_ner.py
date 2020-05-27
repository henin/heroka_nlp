# ======================================================================================================================
# !/usr/bin/env python3
# Filename: heroka_ner.py
# Description: Utility to capture entities for a given sentence based on various NER providers.
# This utility current supports Spacy, NLTK, AWS Comprehend, Google Cloud Language, allennlp
# Author: Henin Karkada <henin.roland@gmail.com>
#        # Python Environment - Python3

# Usage:  python heroka_ner.py <sentence> <spacy|aws|nltk|allen|stanford>"
# Initialisation:

# from heroka_ner import NER
# NER(sentence="George Baker stays in California", process_type="nltk")  
# process_type can be  any of these : <spacy|aws|nltk|allen|stanford>"

# Pre-requisites:
# 1. Set up project path in __init__.py
# 2. Set up client_info in conf/client.json. AWS keys or any other client details

# ======================================================================================================================

import json
import os
import sys

from pprint import pprint

project_path = os.path.sep.join(os.getcwd().split(os.path.sep)[:-2])
sys.path.insert(0, project_path)

from heroka_nlp import logger
from heroka_nlp.src.spacy_nlp import SpacyNLP
from heroka_nlp.src.gcloud_nlp import GCloudNLP
from heroka_nlp.src.allen_nlp import AllenNLP
from heroka_nlp.src.nltk_nlp import NLTKNLP
from heroka_nlp.src.aws_nlp import AWSNLP


class NLP:
    def __init__(self):
        logger.info("Intialising NLP")
        logger.info("If you are using gcloud and refer the documentations for the")


class NER(SpacyNLP, GCloudNLP, AllenNLP, NLTKNLP):

    def __init__(self, **kwargs):

        if kwargs:
            self.process_type = kwargs.get('process_type') if kwargs.get('process_type') else 'spacy'
            self.sentence = kwargs.get('sentence')

        else:
            logger.error("'sentence' param is mandatory!!!")
            sys.exit(1)
        logger.info(f"Recognising Entities using '{self.process_type}' . . . .")
        self.entities = self.ner_process(self.process_type, self.sentence)
        print("\nENTITIES: {}".format(self.entities.get('combined_entities')))

    @staticmethod
    def read_client_info(**kwargs):
        try:
            with open("../conf/client.json") as fp:
                client_info = json.load(fp)

            if kwargs.get('service_type'):
                client_info = client_info[kwargs.get('service_type')]
            if kwargs.get('client'):
                client_info = client_info[kwargs.get('client')]

            return client_info

        except Exception as error:
            logger.error(error)

    def ner_process(self, process_type, sentence):
        """
        Initiated the Respective NER provider object
        Implements the Factory Design Pattern
        :param process_type: process_type can be <spacy|nltk|aws|gcloud|allen>
        :param sentence: Sentence to check for the NER
        :return: entities
        """
        try:
            if process_type == 'gcloud':
                ner = GCloudNLP()
            elif process_type == 'spacy':
                ner = SpacyNLP()
            elif process_type == 'allen':
                ner = AllenNLP()
            elif process_type == 'nltk':
                ner = NLTKNLP()
            elif process_type == 'stanford':
                ner = NLTKNLP()
            elif process_type == 'aws':
                ner = AWSNLP()

            if process_type in ('aws'):
                ner.client_info = self.read_client_info(service_type='ner', client=process_type)
            return ner.recognise_entities(sentence)
            
        except Exception as error:
            logger.error(error)


def main():
    try:
        if len(sys.argv) == 3:
            sentence = sys.argv[1]
            process_type = sys.argv[2]

        elif len(sys.argv) == 2:
            sentence = sys.argv[1]
            process_type = "spacy"
        else:
            logger.info("Usage: python heroka_ner.py <sentence> <spacy|aws|nltk|allen|stanford>")
            sys.exit(1)

        ner = NER(process_type=process_type, sentence=sentence)
        return ner
    except Exception as error:
        logger.error(error)


if __name__ == '__main__':
    main()
