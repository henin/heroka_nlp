from spacy_ner import SpacyNLP
from gcloud_nlp import GCloudNLP
from allen_ner import AllenNLP
from nltk_nlp import NLTKNLP
from aws_nlp import AWSNLP

class NER(SpacyNLP, GCloudNLP, AllenNLP, NLTKNLP):
    def __init__(self):
        pass

    def ner_process(self, text, process_type=None):
        if process_type == 'gcloud':
            obj = GCloudNLP()
        elif process_type == 'spacy':
            obj = SpacyNLP()
        elif process_type == 'allen':
            obj = AllenNLP()
        elif process_type == 'nltk':
            obj = NLTKNLP()
        elif process_type == 'stanford':
            obj = NLTKNLP()
        elif process_type == 'aws':
            obj = AWSNLP()

        results = obj.entities_recognition(text)
        return results

def main():
    pass

if __name__ == '__main__':
    main()
