from collections import defaultdict

from src.models.Document import Document


class Query:
    def __init__(self, id, es_helper, tokens=None):
        self.es_helper = es_helper
        self.id = id
        self.tokens = tokens
        self.update_docs()
        self.calc_query_tf()
        self.docs = {}
        self.query_tf = {}

    def calc_query_tf(self):
        query_words = self.tokens
        query_tf = defaultdict(int)
        for word in query_words:
            query_tf[word] += 1
        self.query_tf = query_tf

    def update_docs(self):
        doc_list = {}
        for token in self.tokens:
            term_docs = self.es_helper.search_term(token)
            for doc in term_docs:
                doc_id = doc['id']
                term_tf_df = self.es_helper.get_tf_df(doc_id, token)
                if (doc_id in list(doc_list.keys())):
                    doc_obj = doc_list[doc_id]
                    doc_obj.add_word(term_tf_df)
                else:
                    doc_obj = Document(doc['id'], doc['dlen'])
                    doc_obj.add_word(term_tf_df)
                    doc_list[doc_id] = doc_obj
        self.docs = doc_list

