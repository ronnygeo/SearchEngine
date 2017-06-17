from src.models.Term import Term


class Document:
    def __init__(self, id, dlen):
        self.id = id
        self.dlen = dlen
        self.terms = []

    def add_word(self, term_obj):
        self.terms.append(term_obj)

    def add_words(self, doc_words):
        for word in doc_words:
            self.add_word(Term(word['term'], word['tf'], word['df']))
