# ======================================================================================================================
# !/usr/bin/env python3
# Filename: nltk_nlp.py
# Description: NLTK related functionalities
# Author: Henin Karkada <henin.roland@gmail.com>
#        # Python Environment - Python3
# Dependencies:
# 1. pip install nltk
# 2. nltk.download('all')
# ======================================================================================================================

import nltk
from collections import defaultdict
from heroka_nlp import logger


class NLTKNLP:

    def __init__(self):
        pass

    def recognise_entities(self, text):
        """
        Recognise entities
        :param text: Sentence
        :return: Entities
        """
        entities_res = defaultdict(list)
        try:
            words = nltk.word_tokenize(text)

            pos_tags = nltk.pos_tag(words)
            chunk = nltk.ne_chunk(pos_tags)
            for ele in chunk.subtrees():
                try:
                    if not ele.label() == 'S':
                        entities_res[ele.label()].append(ele.leaves()[0][0])
                except Exception as err:
                    continue
        except Exception as error:
            logger.error(error)

        return {"combined_entities": dict(entities_res), "metadata": chunk}
