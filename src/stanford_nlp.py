# ======================================================================================================================
# !/usr/bin/env python3
# Filename: stanford_nlp.py
# Description: Standford related functionalities
# Author: Henin Karkada <henin.roland@gmail.com>
#        # Python Environment - Python3


# @TODO: This functionality is not yet created
# Dependencies:
# 1. pip install spacy
# 2. python -m spacy download en_core_web_sm

# ======================================================================================================================


# coding: utf-8

import nltk
from nltk.tag.stanford import StanfordNERTagger

sentence = u"Twenty miles east of Reno, Nev., " \
    "where packs of wild mustangs roam free through " \
    "the parched landscape, Tesla Gigafactory 1 " \
    "sprawls near Interstate 80."

jar = './stanford-ner-tagger/stanford-ner.jar'
model = './stanford-ner-tagger/ner-model-english.ser.gz'

# Prepare NER tagger with english model
ner_tagger = StanfordNERTagger(model, jar, encoding='utf8')

# Tokenize: Split sentence into words
words = nltk.word_tokenize(sentence)

# Run NER tagger on words
print(ner_tagger.tag(words))
