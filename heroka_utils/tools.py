# =======================================================================================
#!/usr/bin/env python3
# Filename: tools.py
# Description: Re-usable functions/methods
# Author: Henin Karkada <henin.karkada@namastecredit.com>
# Python Environment - Python2/Python3
# Usage: To be used as a module
# ========================================================================================
#tools
import calendar
import chardet
import codecs
import csv
import difflib
import inspect
import itertools
import math
import os
import json
import sys
import re
import requests
import time
import types
#import nltk
import shutil
import subprocess as sp
import pymysql.cursors
import pandas as pd
import openpyxl
import boto
import glob
from boto3.s3.transfer import S3Transfer
from copy import deepcopy
from fuzzywuzzy import process, fuzz
from PyPDF2 import PdfFileReader, PdfFileWriter, utils
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from collections import Counter, OrderedDict, Iterable
from datetime import datetime, timedelta
from dateutil import relativedelta
from dateutil.parser import parse
from itertools import tee
from monthdelta import monthdelta
from PIL import Image,ImageSequence, ImageFile
from openpyxl import Workbook
from fpdf import FPDF
import gc
#from ruamel.ordereddict import ordereddict

try:
    from heroka_utils import send_mail
    from heroka_utils import heroka_logging
except Exception as error:
    print(error)

logger = heroka_logging.HerokaLogger().get_logger()

home_path = os.path.expandvars("$HOME")
PROJECT_PATH = os.path.join(home_path, "Documents", "repositories", "utils")

try:
    CREDENTIALS_INFO = os.path.join(PROJECT_PATH, "conf", "credentials.json")
    CLIENT_INFO = os.path.join(PROJECT_PATH, "conf", "client.json")
except Exception as error:
    logger.error(
        "PROJECT_PATH is pointing to {} which doesn't exist locally!!!! Point to the correct folder structure."
        .format(PROJECT_PATH))
    exit(1)



def pairwise(list_items):
    try:
        first_data, second_data = tee(list_items)
        next(second_data, None)
        return zip(first_data, second_data)
    except Exception as error:
        logger.error(error)

def remove_special_chars_digits(list_items):
    for ind, item in enumerate(list_items):
        try:
            item = re.sub(",", "", item)
            try:
                count_decimal = item.count('.')
                if count_decimal > 1:
                    item = re.findall("\d+\.\d+", item)
                    if item:
                        item = item[0]
            except:
                continue
            # @TODO: Remove list of special digits, or conversions : Ex:  zero
            # to 0, or 5 to S
            if item:
                loc_last_decimal = item.rfind('.')
                if loc_last_decimal != -1:
                    list_items[ind] = item[:loc_last_decimal].replace(
                        '.', '') + item[loc_last_decimal:]
                else:
                    list_items[ind] = item
            else:
                continue
        except Exception as error:
            continue
            #logger.warning(error)
    return list_items



def get_list_by_index(item_list, indexes):

    results = []
    for items in item_list:
        try:
            for item in items:
                temp_res = []
                for ind in indexes:
                    try:
                        temp_res.append(items[ind])
                    except:
                        continue
            results.append(temp_res)
        except Exception as error:
            #logger.error(error)
            continue
    return results

def evaluate_datatype_list(list_item, max_one=False,
                           special_char_removal=None):
    datatype_detected = []
    for item in list_item:
        try:
            if isinstance(item, str) and special_char_removal is not None:
                item = eval(item.replace(special_char_removal, ''))

            datatype = type(item)
            datatype_detected.append(datatype.__name__)
        except:
            continue
    if max_one:
        datatype_detected = Counter(datatype_detected).most_common(1)
        if datatype_detected:
            if float(datatype_detected[0][1]) / float(len(list_item)) >= 0.1:
                datatype_detected = datatype_detected[0][0]
                return datatype_detected
            else:
                return None
    else:
        return Counter(datatype_detected)



def flatten_list_list(list_of_list, unique=False):
    try:
        data = list(itertools.chain.from_iterable(list_of_list))
        if unique:
            data = list(set(data))
        return data
    except Exception as error:
        logger.error(error)

def flatten_irregular_list(irregular_list):
    try:
        flattened_list = []
        for el in irregular_list:
            if isinstance(el, Iterable) and not isinstance(el, str):
                for sub in flatten_irregular_list(el):
                    flattened_list.append(sub)
            else:
                flattened_list.append(el)
        return flattened_list
    except Exception as error:
        logger.error(error)


def get_client_info(client, operations=None, sub_operations=None):
    """
    Get client details from the json/database
    :param client: Client name as stored in DB/file
    :return: client_data
    """
    try:
        with open(CLIENT_INFO, 'r') as fp:
            data = json.load(fp)
            client_data = data.get(client)
            if operations:
                client_data = data.get(client).get(operations)
            if operations and sub_operations:
                client_data = data.get(client).get(operations).get(
                    sub_operations)
            return client_data
    except IOError as ie:
        logger.error(
            "The client details/client.json file doesn't exist!!! PROJECT_PATH is currently set to `{}`, please set the proper path!!"
            .format(PROJECT_PATH))
        exit(1)
    except Exception as error:
        logger.error(error)



def check_items_descending(items):
    previous = items[0]
    for number in items:
        if number > previous:
            return False
        previous = number
    return True

def check_items_ascending(items):
    if items[0] <= items[1]:
        return True
    else:
        return False

    #previous = items[0]
    #for number in items:
    #    if number in (737021, '737021'):
    #    if number < previous:
    #        return False
    #    previous = number
    #return True

def similarity_difflib(sent1, sent2):
    """
    Find similarity between two strings using difflib
    :param sent1: String1
    :param sent2: String 2
    :return: Ratio of matching
    """
    try:
        seq = difflib.SequenceMatcher(None, sent1, sent2)
        ratio = seq.ratio() * 100
        return ratio
    except Exception as error:
        logger.error(error)

def zip_with_scalar(list_items, item):
    """
    Zip a single item with each item in the iterable
    :param list_items: Item list
    :param item: Single item
    :return:
    """
    try:
        return list(zip(itertools.repeat(item), list_items))
    except Exception as error:
        logger.error(error)

def unique_permutations_generator(items, permutation_length=2):
    """
    Generate a Unique permutation of  list
    :param items: List of items
    :param permutation_length:  Permutations length if 2: (x,y), if 3 (x,y,z)
    :return:
    """
    try:
        reversed_elements = set()
        for i in itertools.permutations(items, permutation_length):
            if i not in reversed_elements:
                reversed_i = tuple(reversed(i))
                reversed_elements.add(reversed_i)
                # yield i

        return list(reversed_elements)
    except Exception as error:
        logger.error(error)

def unique_permutations_generator2(items, permutation_length=2):
    """
    Generate a Unique permutation of  list
    :param items: List of items
    :param permutation_length:  Permutations length if 2: (x,y), if 3 (x,y,z)
    :return:
    """
    try:
        set_elements = set()
        for p in itertools.permutations(items, 2):
            if p[0] < p[-1]:
                set_elements.add(p)

        return list(set_elements)
    except Exception as error:
        logger.error(error)



def initialise_nltk():
    try:
        stop_words = set(stopwords.words('english'))
        tokenizer = RegexpTokenizer(r'[a-zA-Z]+')
        month_stop_words_abbr = list(calendar.month_abbr)
        month_stop_words_abbr = [
            month + ' ' for month in month_stop_words_abbr
        ]
        monthname_stop_words = list(
            calendar.month_name) + month_stop_words_abbr
        monthname_stop_words = list([_f for _f in monthname_stop_words if _f])
        stop_words_month = '|'.join(monthname_stop_words)
        month_regex = re.compile(stop_words_month, flags=re.IGNORECASE)
        other_stop_words = {'xx', 'india'}
        stop_words.update(other_stop_words)
    except Exception as error:
        logger.error(error)
    return stop_words, tokenizer, monthname_stop_words, month_regex

def process_clean_text(sentence1,
                       stop_words,
                       tokenizer,
                       month_regex,
                       sentence2=None):
    """
    Find the similarity between two sentences
    :param sentence1: Sentence 1 string
    :param sentence2: Sentence2 string
    :return:
    """
    try:
        if sentence1:
            try:
                # sentence1_word_tokens = " ".join(tokenizer.tokenize(sentence1))
                # sentence1_word_tokens = word_tokenize(sentence1_word_tokens)
                #
                # sentence1_filtered_sentence = {w for w in
                #                                sentence1_word_tokens if not
                #                                w.lower() in stop_words}
                #
                # result1_pos = nltk.pos_tag(sentence1_filtered_sentence)
                result1 = month_regex.sub(" ", sentence1)
                result1 = " ".join([
                    res for res in tokenizer.tokenize(result1)
                    if not res.lower() in stop_words
                ])

            except Exception as error:
                logger.warning(error)
                result1 = sentence1

        if sentence2:
            try:
                # sentence2_word_tokens = " ".join(tokenizer.tokenize(sentence2))
                # sentence2_word_tokens = word_tokenize(sentence2_word_tokens)
                #
                # sentence2_filtered_sentence = {w for w in
                #                                sentence2_word_tokens if not
                #                                w.lower() in stop_words}
                #
                # result2_pos = nltk.pos_tag(sentence2_filtered_sentence)
                result2 = month_regex.sub(" ", sentence2)
                result2 = " ".join([
                    res for res in tokenizer.tokenize(result2)
                    if not res.lower() in stop_words
                ])

            except Exception as error:
                logger.warning(error)
                result2 = sentence2

        if sentence1 and not sentence2:
            return result1

        elif sentence2 and not sentence1:
            return result2

        elif sentence1 and sentence2:
            return result1, result2

    except Exception as error:
        logger.error(error)
        return

def sentence_similarity_score(comparison_sentence, list_sentence_to_compare,
                              limit, basic_threshold, stop_words, tokenizer,
                              month_regex):
    """
    Find Similarity between a sentence and list of sentences to be compared
    using Fuzzy wuzzy Process One
    :param comparison_sentence : Single sentence
    :param list_sentence_to_compare: List of sentences
    :param limit: Results to be returned
    :param basic_threshold: Basic threshold score to  filter results
    :return:
    """
    try:
        threshold_value = []
        similarity_threshold = []
        list_sentence_to_compare_cleaned = OrderedDict()
        comparison_sentence_cleaned = process_clean_text(
            str(comparison_sentence), stop_words, tokenizer, month_regex)
        if not comparison_sentence_cleaned:
            return None, None

        for sentence in list_sentence_to_compare:
            try:
                list_sentence_to_compare_cleaned[
                    sentence] = process_clean_text(
                        str(sentence), stop_words, tokenizer, month_regex)
            except Exception as error:
                logger.warning(error)
                continue

        list_sentence_to_compare = list(
            list_sentence_to_compare_cleaned.values())
        list_sentence_to_compare = list(
            [_f for _f in list_sentence_to_compare if _f])
        process_results = process.extract(
            comparison_sentence_cleaned,
            list_sentence_to_compare,
            scorer=fuzz.token_set_ratio,
            limit=limit)
        for sent_res in process_results:
            try:
                list_sentence_to_compare_key = \
                    list(list_sentence_to_compare_cleaned.keys())[list(list_sentence_to_compare_cleaned.values()).index(sent_res[0])]
                if sent_res[1] >= basic_threshold:
                    threshold_value.append(sent_res[1])
                    # sent_res[1] - Scorre of match, comparison_sentence-sentence1, list_sentence_to_compare_key = Sentence2,
                    # comparison_sentence_cleaned - Sentence1 cleaned,sent_res[0] - Sentence 2 cleaned
                    similarity_threshold.append(
                        (sent_res[1], comparison_sentence,
                         list_sentence_to_compare_key,
                         comparison_sentence_cleaned, sent_res[0]))
                del list_sentence_to_compare_cleaned[
                    list_sentence_to_compare_key]
            except Exception as error:
                logger.warning(error)
                continue
        if threshold_value:
            # Same statement match value should be removed
            if threshold_value.count(100) > 4:
                threshold_value.remove(100)
            sentence_similarity_score = sum(threshold_value) / len(
                threshold_value)
        else:
            sentence_similarity_score = 100
    except Exception as error:
        logger.error(error)
        sentence_similarity_score = 100

    # Clear variables
    list_sentence_to_compare = []
    process_results = []
    threshold_value = []
    list_sentence_to_compare_cleaned = OrderedDict()
    return sentence_similarity_score, similarity_threshold

####################################################################
# Generic functions for debugging purposes
def whoami(script_name=None):
    """ Get the current functionality name in a particular module
n        :param script_name: The name of script
        :return:
        """

    if not script_name:
        script_name = os.path.basename(__file__)

    return inspect.stack()[1][3]

def list_user_defined_functions(script_name=None):
    """
    Lists the names of user defined functionality in a particular module
    :param script_name: The name of script
    :return:
    """
    if not script_name:
        script_name = os.path.basename(__file__)
    user_defined_functions = [f.__name__ for f in list(globals().values()) if
                              type(f) == \
                              types.FunctionType]
    logger.info("The list of functions in {} script are {}:".format(
        script_name, user_defined_functions))

def reload_ipython():
    import imp
    imp.reload("module_name")

def convert_bytes(num):
    """
    Function will convert bytes to MB.... GB... etc
    Param: bytes
    return conveted units

    """
    try:
        for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
            if num < 1024.0:
                return "%3.1f %s" % (num, x)
            num /= 1024.0
    except Exception as error:
        logger.error(error)
