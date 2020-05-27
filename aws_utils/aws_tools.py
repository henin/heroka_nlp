# ======================================================================================================================
# !/usr/bin/env python3
# Filename: heroka_ner.py
# Description:
# Author: Henin Karkada <henin.roland@gmail.com>
#        # Python Environment - Python3
# Usage: python heroka_ner.py 
#
# Usage: 

#

# ======================================================================================================================


from heroka_nlp import logger

import sys

try:
    import boto3
except ImportError as ie:
    logger.error(ie)
    sys.exit(1)


class AWS:

    def __init__(self, **kwargs):

        if kwargs:
            self.service = kwargs.get('service')
            self.service_type = kwargs.get('service_type', 'resource')

    def get_service(self,
                    service_name,
                    service_type=None,
                    service_region_name=None,
                    aws_access_key_id=None,
                    aws_secret_access_key=None):
        """
        Returns the AWS service details when provided the service name
        :param service_name:
        :return:
        """
    
        try:
            # @TODO: To be removed
            if aws_access_key_id and aws_secret_access_key:
                credentials = {
                'aws_access_key_id': aws_access_key_id,
                'aws_secret_access_key': aws_secret_access_key
                }

        except KeyError as key_error:
            logger.error(key_error)

        if service_type == 'client':
            return boto3.client(service_name, service_region_name, **credentials)
        else:
            return boto3.resource(service_name, service_region_name, **credentials)
