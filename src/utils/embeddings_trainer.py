
import nltk
import preprocessor as tw_preprocessor
import tempfile

from gensim.models import Word2Vec


tw_preprocessor.set_options(tw_preprocessor.OPT.URL, 
                            tw_preprocessor.OPT.MENTION,
                            tw_preprocessor.OPT.RESERVED,
                            tw_preprocessor.OPT.NUMBER,
                            tw_preprocessor.OPT.EMOJI)

class EmbeddingsTrainer:
    
    corpus = []
    model = None
    tokenizer = nltk.RegexpTokenizer(r"\w+")

    def __init__(self, docs):
        stopwords = nltk.corpus.stopwords.words("spanish") + \
                    nltk.corpus.stopwords.words("english")

        for doc in docs:
            doc = doc.lower()
            doc = tw_preprocessor.clean(doc)
            text = []
            for word in self.tokenizer.tokenize(doc):
                if word not in stopwords and not str.isdigit(word):
                    text.append(word)
            self.corpus.append(text)
    
    def save_model(self, model_fn):
        with tempfile.NamedTemporaryFile(prefix='embeddings-model-', 
                                         delete=False) as tmp:
            temp_filepath = tmp.name
            self.model.save(temp_filepath)

    def load_model(self, model_fn):
        self.model = Word2Vec.load(model_fn)

    def train(self, min_count=3, vec_size=200, workers=1):
        self.model = Word2Vec(self.corpus, min_count=min_count, 
                              vector_size=vec_size,
                              workers=workers)
    
    def find_similar(self, terms, max_similar):
        similar_terms = self.model.wv.most_similar(positive=terms, 
                                                   topn=max_similar)
        return similar_terms
    
    def find_disimilar(self, terms, max_disimilar):
        disimilar_terms = self.model.wv.most_similar(negative=terms, 
                                                     topn=max_disimilar)
        return disimilar_terms
