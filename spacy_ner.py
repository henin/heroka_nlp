#pip install spacy
#python -m spacy download en_core_web_sm
import spacy
nlp = spacy.load("en_core_web_sm")

class SpacyNLP():
    # Load English tokenizer, tagger, parser, NER and word vectors
    def __init__(self):

        print("Into init")

    def entities_recognition(self, text):
        doc = nlp(text)
        # Analyze syntax
        noun_phrases = [chunk.text for chunk in doc.noun_chunks]
        verbs = [token.lemma_ for token in doc if token.pos_ == "VERB"]
        #for entity in doc.ents:
        ner_text_label = [(entity.text, entity.label_) for entity in doc.ents]
        return {'noun_phrases': noun_phrases, 'verbs': verbs, 'ner_text_label0': ner_text_label}
