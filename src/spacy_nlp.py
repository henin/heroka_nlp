# ======================================================================================================================
# !/usr/bin/env python3
# Filename: spacy_nlp.py
# Description: Spacy related functionalities
# Author: Henin Karkada <henin.roland@gmail.com>
#        # Python Environment - Python3

# Dependencies:
# 1. pip install spacy
# 2. python -m spacy download en_core_web_sm

# ======================================================================================================================

from heroka_nlp import logger
from collections import defaultdict
try:
    import spacy
except ImportError as ie:
    logger.error(ie)


class SpacyNLP():
    # Load English tokenizer, tagger, parser, NER and word vectors
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")


    def recognise_entities(self, text):
        try:
            combined_entities = defaultdict(list)
            doc = self.nlp(text)
            # Analyze syntax
            noun_phrases = [chunk.text for chunk in doc.noun_chunks]
            verbs = [token.lemma_ for token in doc if token.pos_ == "VERB"]
            #for entity in doc.ents:
            [combined_entities[entity.label_].append(entity.text) for entity in doc.ents]

            return {'combined_entities': dict(combined_entities), 'metadata': {'noun_phrases': noun_phrases, 'verbs': verbs}}
        except Exception as error:
            logger.error(error)


if __name__ == '__main__':
    pass
