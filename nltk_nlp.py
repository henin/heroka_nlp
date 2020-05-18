import nltk
from collections import defaultdict

class NLTKNLP():

    def __init__(self):
        pass

    def entities_recognition(self, text):
        entities_res = defaultdict(list)
        try:
            words = nltk.word_tokenize(text)

            pos_tags = nltk.pos_tag(words)
            chunk = nltk.ne_chunk(pos_tags)
            for ele in chunk.subtrees():
                if not ele.label() == 'S':
                    entities_res[ele.label()].append(dict(ele.leaves()))

        except:
            pass

        return dict(entities_res)
