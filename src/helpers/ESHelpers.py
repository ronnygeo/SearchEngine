from elasticsearch import Elasticsearch, RequestsHttpConnection

ELASTIC_USER = "elastic"
ELASTIC_PWD = "changeme"
ELASTIC_INDEX = "ap_dataset"
ELASTIC_TYPE = "document"
DF_THRESHOLD = 7000
# Delete from Query
# DELETE_LIST = ["document"]
DELETE_LIST = [
    "document", "discuss", "type", "identifi", "predict", "cite", "describ",
    "determin"
]

class ESHelpers:
    """ Utility Class with all ES helper functions """

    def __init__(self):
        self.es = Elasticsearch(connection_class=RequestsHttpConnection, http_auth=(ELASTIC_USER, ELASTIC_PWD))
        self.get_avg_dlen()
        self.total_docs()
        self.get_sum_ttf()
        self.get_vocab_size()

    def get_tokens(self, query):
        """
        Creates tokens for a query.
        @param: A query that needs to be tokenized
        This function first analyzes the query and gets the tokens and filters with the DELETE LIST and
        only displays the tokens that have a Document Frequency less than the threshold.
        """
        data = self.es.indices.analyze(index=ELASTIC_INDEX, analyzer="my_english", body={"text": query})["tokens"]
        filtered_data = list(map(lambda x: x["token"], data))
        new_tokens = []
        for word in filtered_data:
            if (self.get_df(word) < DF_THRESHOLD):
                new_tokens.append(word)
        return list(filter(lambda x: x not in DELETE_LIST, new_tokens))

    def get_avg_dlen(self):
        """ Gets the average document length from elasticsearch"""
        query_aggs = {
            "query": {
                "match_all": {}
            },
            "aggs": {
                "doc_lengths": {
                    "stats": {
                        "script": {
                            "lang": "groovy",
                            "inline": "_doc['doc_length']"
                        }
                    }
                }
            }
        }
        self.avg_dlen = \
        self.es.search(index=ELASTIC_INDEX, doc_type=ELASTIC_TYPE, body=query_aggs)["aggregations"]['doc_lengths'][
            'avg']

    def get_vocab_size(self):
        """ Gets the average document length from elasticsearch"""
        query_cardinality = {
            "size": 0,
            "aggs": {
                "vocabSize": {
                    "cardinality": {
                        "field": "text"
                    }
                }
            }
        }
        self.vocab_size = \
        self.es.search(index=ELASTIC_INDEX, doc_type=ELASTIC_TYPE, body=query_cardinality)["aggregations"]['vocabSize'][
            'value']

    def get_sum_ttf(self):
        """ Gets the sum of total tf from elasticsearch"""
        self.sum_ttf = self.es.field_stats(index=ELASTIC_INDEX, fields="text")['indices']['_all']['fields']['text'][
            'sum_total_term_freq']

    def total_docs(self):
        """ Gets the total number of documents in elasticsearch """
        self.doc_count = \
        self.es.field_stats(index=ELASTIC_INDEX, fields=['doc_length'])['indices']['_all']['fields']['doc_length'][
            'doc_count']

    def get_all_tv(self, doc, body=None):
        """ Gets all the TermVectors for a particular document.
            @param: A document Id
        """
        if body:
            return \
            self.es.termvectors(index=ELASTIC_INDEX, doc_type=ELASTIC_TYPE, id=doc, positions=False, payloads=False,
                                offsets=False, term_statistics=True, body=body)['term_vectors']['text']['terms']
        else:
            return \
            self.es.termvectors(index=ELASTIC_INDEX, doc_type=ELASTIC_TYPE, id=doc, positions=False, payloads=False,
                                offsets=False, term_statistics=True)['term_vectors']['text']['terms']

    def get_tf_df(self, doc, term):
        """
        Get TF and DF values for the given term in the given document
        @param term: A term
        @param doc: A document Id
        """
        tv_doc = self.get_all_tv(doc)[term]
        return Term(term, tv_doc['term_freq'], tv_doc['doc_freq'], tv_doc['ttf'])

    def get_df_2(self, term):
        """
        Get the Document Frequency for the given term
        @param term: term to get DF
        """
        df_search_qry = {
            "script_fields": {
                "df": {
                    "script": {
                        "lang": "groovy",
                        "inline": "_index['text']['" + term + "'].df()"
                    }
                }
            }
        }
        return \
        self.es.search(index=ELASTIC_INDEX, doc_type=ELASTIC_TYPE, body=df_search_qry)['hits']['hits'][0]['fields'][
            'df'][0]

    def get_ttf(self, term):
        """
        Get the Total Term Frequency for the given term
        @param term: term to get DF
        """
        ttf_search_qry = {
            "script_fields": {
                "ttf": {
                    "script": {
                        "lang": "groovy",
                        "inline": """_index['text']["%s"].ttf()""" % term
                    }
                }
            }
        }
        return \
        self.es.search(index=ELASTIC_INDEX, doc_type=ELASTIC_TYPE, body=ttf_search_qry)['hits']['hits'][0]['fields'][
            'ttf'][0]

    def get_df(self, term):
        """
        Get the Document Frequency for the given term. Different implementation
        @param term: term to get DF
        """
        return self.es.count(index=ELASTIC_INDEX, doc_type=ELASTIC_TYPE, body={"query": {"term": {"text": term}}})[
            'count']

    def search_term(self, term):
        """
        Find all documents containing the given term. Performs an exact match.
        @param term: A term to search in ES
        """
        #         size=self.get_df(term)
        search_qry = {
            "query": {
                "term": {
                    "text": term
                }
            }
        }
        res = self.es.search(index=ELASTIC_INDEX, doc_type=ELASTIC_TYPE, _source=["_id", "doc_length"], size=9000,
                             body=search_qry)
        return (list(map(lambda x: {"id": x['_id'], "dlen": x['_source']['doc_length']}, res['hits']['hits'])))

    def search(self, query):
        """
        Performs a search on ES with the given query
        @param query: A query to perform search
        """
        return self.es.search(index=ELASTIC_INDEX, doc_type=ELASTIC_TYPE, body=query)

    def get_all_tv_multi(self, doc_ids, body=None):
        """
        Gets all the TermVectors from multiple documents
        @param doc_ids: A list of document Ids to fetch the termvectors
        """
        if body:
            return self.es.mtermvectors(index=ELASTIC_INDEX, doc_type=ELASTIC_TYPE, offsets="false", payloads="false",
                                        positions="false", term_statistics="true", field_statistics="true", ids=doc_ids,
                                        body=body)['docs']
        else:
            return self.es.mtermvectors(index=ELASTIC_INDEX, doc_type=ELASTIC_TYPE, offsets="false", payloads="false",
                                        positions="false", term_statistics="true", field_statistics="true",
                                        ids=doc_ids)['docs']

    def get_multi_tf(self, term, doc_ids):
        """
        Get the TF for given term from the given document IDs
        @param term: A term to get TF
        @param doc_ids: A list of document Ids to fetch TF
        """
        res = self.get_all_tv_multi()
        return list(map(
            lambda x: {"id": x['_id'], "term": term, "tf": x['term_vectors']['text']['terms'][term]['term_freq'],
                       "df": x['term_vectors']['text']['terms'][term]['doc_freq']}, res))

    def get_significant_terms(self, term):
        sign_term_qry = {
            "query": {
                "term": {"text": term}
            },
            "aggregations": {
                "sign_term": {
                    "significant_terms": {
                        "field": "text"
                    }
                }
            },
            "size": 0
        }
        return \
        self.es.search(index=ELASTIC_INDEX, doc_type=ELASTIC_TYPE, body=sign_term_qry)['aggregations']['sign_term'][
            'buckets']
