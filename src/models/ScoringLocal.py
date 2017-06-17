import re
import os
import math
import copy
import operator

from src.indexer.IndexUtils import IndexUtils
from src.models.QueryLocal import Query
from src.models.Term import Term


class Scoring:
    """
        A class to load the queries, and create the initial dataset from ES.
        then performs multiple scoring techniques with different models.
    """
    def __init__(self, search, queries=None):
        self.results = {}
        self.search = search

        self.avgdlen = self.search.getAvgDLen()
        self.num_docs = self.search.getNumDocs()
        self.vocab_size = self.search.getVocabSize()
        self.sum_ttf = self.search.getSumTTF()
        print(self.sum_ttf)

        if queries:
            self.queries = queries

    def load_queries(self, file):
        """
        Load the queries from the given file and tokenize them
        @param file: A file name to fetch the queries from
        """
        queries = []
        utils = IndexUtils()
        with open(file, 'r') as f:
            for line in f:
                reg_match = re.match(r'^(\d+).(.*)', line)
                tokens = utils.preprocess(reg_match.group(2).strip())
                queries.append(Query(reg_match.group(1).strip(), self.search, tokens))
        self.queries = queries
        
    def logTF(self, tf):
        """
        Calculates the log of the given TF value
        @param tf: A Term Frequency value
        """
        return math.log(tf)

    def okapiTF(self, tf, dlen, avgdlen):
        """
        Calculates the okapiTF value from the given parameters.
        @param tf: Term Frequency
        @param dlen: Document Length
        @param avgdlen: Average document length in ES
        """
        return tf / (tf + 0.5 + 1.5 * (dlen/avgdlen))

    def processVS(self, model="okapi", l = 0.5):
        """
        Transforms the input dataset to a new retrieval model.
        @param model: A retrieval model to use.
        Options: okapi, tfidf, bm25
        """
        self.results[model] = {}
        for query in self.queries:
            res = {}
            for doc in query.docs.values():
                sum_otf = 0.0
                for term in doc.terms:
                    if model == "okapi":
                        sum_otf += self.okapiTF(term.tf, doc.dlen, self.avgdlen)
                    if model == "tfidf":
                        sum_otf += self.tfidf(term.tf, term.df, doc.dlen, self.avgdlen, self.num_docs)
                    if model == "bm25":
                        sum_otf += self.bm25(term.tf, term.df, query.query_tf[term.term], doc.dlen, self.avgdlen, self.num_docs)
                res[doc.id] = sum_otf
            self.results[model][query.id] = res
            
    def processLM(self, model='laplace'):
        self.results[model] = {}
        for query in self.queries:
            res = {}
            df = {}
            ttf = {}
            
            for token in query.tokens:
                search_res = self.search.searchTerm(token)
                if search_res:
                    docs, df_term, ttf_term = search_res
                    df[token] = df_term
                    ttf[token] = ttf_term
                else:
                    df[token] = 0
                    ttf[token] = 0
                
            local_query = copy.deepcopy(query)
            for doc in local_query.docs.values():
                sum_otf = 0.0
                for token in query.tokens:
                    if token not in map(lambda t: t.term, doc.terms):
                        # search_res2 = self.search.searchTerm(token)
                        # if search_res2:
                        #     _, df_t, ttf_t = search_res2
                        #     doc.terms.append(Term(token, 0, df_t, ttf_t, []))
                        # else:
                        doc.terms.append(Term(token, 0, df[token], ttf[token], []))
                for term in doc.terms:
                    if model == "laplace":
                        sum_otf += self.ulm_laplace(term.tf, doc.dlen)
                    if model == "jmercer":
                        sum_otf += self.ulm_mercer(term.tf, term.ttf, doc.dlen, 0.5)

                res[doc.id] = sum_otf
            self.results[model][query.id] = res

    def proximitySearch(self):
        self.results["proximity"] = {}
        for query in self.queries:
            res = {}
            for doc in query.docs.values():
                sum_range = 0.0
                positions = list(map(lambda t: t.pos, doc.terms))
                num_terms = len(positions)
                if num_terms > 1:
                    for i in range(num_terms):
                        j = i + 1
                        if j < num_terms:
                            sum_range += self.PS2(positions[i], positions[j])

                res[doc.id] = self.proximityScore(sum_range, num_terms, self.search.getDLen(doc), self.vocab_size)
            self.results["proximity"][query.id] = res

    def proximitySearchCombined(self):
        self.processLM("okapi")
        self.results["proximityCombined"] = {}
        for query in self.queries:
            res = {}
            for doc in query.docs.values():
                sum_range = 0.0
                positions = list(map(lambda t: t.pos, doc.terms))
                num_terms = len(positions)
                if num_terms > 1:
                    for i in range(num_terms):
                        j = i + 1
                        if j < num_terms:
                            sum_range += self.PS2(positions[i], positions[j])

                try:
                    res[doc.id] = self.proximityScore(sum_range, num_terms, self.search.getDLen(doc), self.vocab_size) + self.results["okapi"][query.id][doc.id]
                    # print("got in okapi")

                except Exception:
                    # print(self.results["okapi"][query.id][doc.id])
                    res[doc.id] = self.proximityScore(sum_range, num_terms, self.search.getDLen(doc), self.vocab_size)
            self.results["proximityCombined"][query.id] = res

    def PS2(self, pos1, pos2):
        i1 = 0
        i2 = 0
        item1 = int(pos1[i1])
        item2 = int(pos2[i2])
        rn = int(item2) - int(item1)
        while i1 < len(pos1) and i2 < len(pos2):
            hit1 = int(pos1[i1])
            hit2 = int(pos2[i2])
            diff = int(hit2) - int(hit1)
            if hit1 < hit2:
                if rn > diff:
                    rn = diff
                i1 += 1
            else:
                i2 += 1
        return rn

    def proximityScore(self, range_window, num_terms, dlen, V, C = 1500):
        return (C - range_window) * num_terms / (dlen + V)

    def output_results(self, model, limit=1000):
        """
        Extract the final list with new values
        @param limit: A limit 
        """
        out = []
        for key in self.results[model].keys():
            qId = key
            count = 1
            sorted_docs = sorted(self.results[model][key].items(), key=operator.itemgetter(1), reverse=True)[:limit] 
            for  val in sorted_docs:
                dId = val[0]
                dScore = val[1]
                dRank = count
                out.append(str(qId) + " " + "Q0" + " " + str(dId) + " " + str(dRank) + " " + str(dScore) + " " + "Exp")
                count += 1
        return out
    
    def write_file(self, model, fname, limit=1000):
        with open(fname, 'w') as outFile:
            for data in self.output_results(model, limit):
                outFile.write(data + os.linesep)

    def tfidf(self, tf, df, dlen, avgdlen, num_docs):
        temp = self.okapiTF(tf, dlen, avgdlen)
        return temp * math.log(num_docs/df)

    def bm25(self, tf, df, q_tf, dlen, avgdlen, num_docs, k1=1.2, k2=0.5, b=0.75):
        return math.log((num_docs+0.5)/(df+0.5)) * ((tf + k1*tf)/(tf+k1 * ((1-b) + b * dlen/avgdlen))) * (q_tf + k2*q_tf)/(q_tf + k2)

    def ulm_laplace(self, tf, dlen):
        return math.log((tf + 1) / (dlen + self.vocab_size))

    def ulm_mercer(self, tf, ttf, dlen, l=0.5):
        return math.log(l * tf / dlen + (1 - l) * (ttf - tf) / (self.sum_ttf - dlen))
