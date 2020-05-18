#pip install spacy
#python -m spacy download en_core_web_sm
from collections import defaultdict
import requests
import time
import sys
from utils.logger_settings import *
class AllenNLP():
    # Load English tokenizer, tagger, parser, NER and word vectors
    def __init__(self):

        print("Into init")

    def entities_recognition(self, listofnames, entities=None):
        print(listofnames)
        #Create a Default dict
        names_entity = defaultdict(list)
        for name in listofnames:
            print(name)
            try:
                #Api Approcach
                url = "https://demo.allennlp.org/predict/named-entity-recognition"
                url = "https://demo.allennlp.org/api/named-entity-recognition/predict"
                #defining a params dict for the parameters to be sent to the API
                payload = {"sentence": name}
                import pdb;pdb.set_trace()
                # sending get request and saving the response as response object
                try:
                    post_response = requests.post(url=url, json=payload)
                except:
                    time.sleep(1)
                    post_response = requests.post(url=url, json=payload)
                if post_response.status_code != 200:
                    logger.error("Unable to hit ALLEN NLP API!!!!")
                    sys.exit(1)

                results = post_response.json()
                if results:
                    print(results['tags'], results['words'])
                    names_entity[name] = {res for res in zip(results['tags'],  results['words']) if not res[0] == 'O'}
                            # word = word.strip()
                        #     #removing unwanted tags for kyc
                        #     if tag != 'O' and tag.split('-')[-1] != 'ORG' and tag.split('-')[-1] != 'MISC' and len(
                        #                     word) > 3 and word.isalpha():
                        #         names_entity[tag].append(word)
                        # except:
                #             continue
                # else:
                #     continue
            except Exception as error:
                print(error)
                continue

        if entities:
            requested_entities = {}
            for entity in entities:
                requested_entities[entity] = names_entity[entity]
            return dict(requested_entities)
        else:
            return dict(names_entity)
