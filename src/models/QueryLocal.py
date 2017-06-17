from collections import defaultdict

from src.indexer.IndexUtils import IndexUtils
from src.models.Document import Document
from src.models.Term import Term

DELETE_LIST = [
    "document", "docu", "discuss", "type", "identifi", "predict", "cite", "describ",
    "determin"
]

class Query:
    def __init__(self, id, search, text=None, threshold=10000, stemmer="porter"):
        self.search = search
        self.id = id
        self.utils = IndexUtils()
        self.threshold = threshold
        if stemmer:
            self.tokens = list(filter(lambda x: x not in DELETE_LIST, self.utils.preprocess(text, True, stemmer)))
        else:
            self.tokens = list(filter(lambda x: x not in DELETE_LIST, self.utils.preprocess(text, False)))
        self.docs = {}
        self.query_tf = {}
        self.update_docs()
        self.calc_query_tf()


    def calc_query_tf(self):
        query_words = self.tokens
        query_tf = defaultdict(int)
        for word in query_words:
            query_tf[word] += 1
        self.query_tf = query_tf

    def update_docs(self):
        doc_list = {}
        print(self.tokens)
        for token in self.tokens:
            search_res = self.search.searchTerm(token)
            if search_res:
                term_docs, df, ttf = search_res
                if df < self.threshold:
                    for docId, values in term_docs.items():
                        term_tf = values["tf"]
                        t = Term(token, term_tf, df, ttf, values["pos"])
                        if docId in list(doc_list.keys()):
                            doc_obj = doc_list[docId]
                            doc_obj.add_word(t)
                        else:
                            doc_obj = Document(docId, self.search.getDLen(docId))
                            doc_obj.add_word(t)
                            doc_list[docId] = doc_obj
        self.docs = doc_list