import re
import os
import math
import copy
import operator



class Scoring:
    """
        A class to load the queries, and create the initial dataset from ES.
        then performs multiple scoring techniques with different models.
    """
    def __init__(self, es=None, queries=None):
        self.es_helper = es
        self.avgdlen = self.es_helper.avg_dlen
        self.num_docs = self.es_helper.doc_count
        self.vocab_size = self.es_helper.vocab_size
        self.sum_ttf = self.es_helper.sum_ttf
        if queries: self.queries = queries
        self.results = {}
        
    def load_queries(self, file):
        """
        Load the queries from the given file and tokenize them
        @param file: A file name to fetch the queries from
        """
        queries = []
        with open(file, 'r') as f:
            for line in f:
                reg_match = re.match(r'^(\d+).(.*)', line)
                tokens = self.es_helper.get_tokens(reg_match.group(2).strip())
                queries.append(Query(reg_match.group(1).strip(), self.es_helper, tokens))
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
            
    def processLM(self, model='laplace', l=0.5):
        self.results[model] = {}
        for query in self.queries:
            res = {}
            df = {}
            ttf = {}
            
            for token in query.tokens:
                df[token] = self.es_helper.get_df(token)
                ttf[token] = self.es_helper.get_ttf(token)
                
            local_query = copy.deepcopy(query)
#             print("Query tokens: ", query.tokens)
            for doc in local_query.docs.values():
                sum_otf = 0.0
#                 print(doc.dlen, self.vocab_size)
#                 print("doc terms before: ", list(map(lambda t: t.term, doc.terms)))
                for token in query.tokens:
                    if token not in map(lambda t: t.term, doc.terms):
                        doc.terms.append(Term(token, 0, df[token], ttf[token]))
#                 print("doc terms after: ", list(map(lambda t: (t.term, t.tf, t.df, t.ttf), doc.terms)))
                for term in doc.terms:
                    if model == "laplace":
                        sum_otf += self.ulm_laplace(term.tf, doc.dlen)
                    if model == "jmercer":
                        sum_otf += self.ulm_mercer(term.tf, term.ttf, doc.dlen, l)
                res[doc.id] = sum_otf
            self.results[model][query.id] = res
                
    def output_results(self, model, limit):
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
        with open(fname, 'a') as outFile:
            for data in self.output_results(model, limit):
                outFile.write(data + os.linesep)
        
    def tf_doc_query(self, doc, query, tfidf=False):
        sumtf = 0.0
        #tokenize query
        for word in query:
            tf = getTF(doc, word)
            temp = okapiTF(tf, dlen, avgdlen)
            if tfidf:
                temp * log(es_helper.doc_count/df)
            sumtf += temp
        return sumtf

    def tfidf(self, tf, df, dlen, avgdlen, num_docs):
        temp = self.okapiTF(tf, dlen, avgdlen)
        return temp * math.log(num_docs/df)
            
    def bm25(self, tf, df, q_tf, dlen, avgdlen, num_docs, k1=1.2, k2=0.5, b=0.75):
        return (math.log((num_docs+0.5)/(df+0.5)) * ((tf + k1*tf)/(tf+k1 * ((1-b) + b * dlen/avgdlen))) * (q_tf + k2*q_tf)/(q_tf + k2))

    def ulm_laplace(self, tf, dlen):
        return math.log((tf + 1) /(dlen + self.vocab_size))
    
    def ulm_mercer(self, tf, ttf, dlen, l=0.5):
#         return math.log(l * tf/dlen + (1-l) * ttf/self.vocab_size)
        return math.log(l * tf/dlen + (1-l) * (ttf - tf)/(self.sum_ttf - dlen))

# min(list(scoring.results["okapi"]["100"].values()))
    def normalize(self):
        for model in self.results.keys():
            model_min = 99999
            model_max = -99999
            for qId, docs in self.results[model].items():
#                 print("docs", docs)
                model_min = min(model_min, min(list(docs.values())))
                model_max = max(model_max, max(list(docs.values())))            
                model_diff = model_max - model_min
            print(model_diff)
            for qId in self.results[model].keys():
                for k, v in docs.items():
                    self.results[model][qId][k] = (v - model_min)/model_diff
                
    def metasearch(self, limit=1000):
        out = []
        docs = {}
        for model in self.results.keys():
            for qId, d in self.results[model].items():
                docs[qId] = {}
                for docId, val in d.items():
                    docs[qId][docId] = 0.0
        for model in self.results.keys():
            for qId, d in self.results[model].items():
                for docId, val in d.items():
                    docs[qId][docId] += val
        for qId, d in docs.items():
            sorted_docs = sorted(d.items(), key=operator.itemgetter(1), reverse=True)[:limit]
            count = 1
            for  val in sorted_docs:
                    dId = val[0]
                    dScore = val[1]
                    dRank = count
                    out.append(str(qId) + " " + "Q0" + " " + str(dId) + " " + str(dRank) + " " + str(dScore) + " " + "Exp")
                    count += 1
        return out
    
    def write_meta_file(self, fname, limit=1000):
        with open(fname, 'a') as outFile:
            for data in self.metasearch(limit):
                outFile.write(data + os.linesep)


