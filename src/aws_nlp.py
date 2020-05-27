# ======================================================================================================================
# !/usr/bin/env python3
# Filename: aws_nlp.py
# Description:
# Author: Henin Karkada <henin.roland@gmail.com>
#        # Python Environment - Python3
# Dependencies:
# 1. pip install boto3

# ======================================================================================================================
import sys
from collections import defaultdict


from aws_utils.aws_tools import AWS
from heroka_nlp import logger

try:
    import boto3
except ImportError as ie:
    logger.error(ie)
    sys.exit(1)

# Standard imports

class AWSNLP(AWS):
    def __init__(self):
        pass

    def recognise_entities(self, text):
        try:

            combined_entities = defaultdict(list)
            comprehend = super().get_service(service_name="comprehend",
                                       service_type="client",
                                       service_region_name=self.client_info['region'],
                                       aws_access_key_id=self.client_info['aws_access_key_id'],
                                       aws_secret_access_key=self.client_info['aws_secret_access_key'])

            for item in comprehend.detect_entities(Text=text, LanguageCode='en')['Entities']:
                combined_entities[item['Type']].append(item['Text'])    
            return {'combined_entities': dict(combined_entities), 'metadata': comprehend.detect_entities(Text=text, LanguageCode='en')['Entities']}
        except Exception as error:
            logger.error(error)
