# coding: utf-8

# In[1]:

from bs4 import BeautifulSoup
import re
import os
from elasticsearch import Elasticsearch, helpers
from datetime import datetime

# In[2]:

ELASTIC_USER = "elastic"
ELASTIC_PWD = "changeme"
ELASTIC_INDEX = "ap_dataset"
ELASTIC_TYPE = "document"

# In[3]:

from elasticsearch import Elasticsearch, RequestsHttpConnection

es = Elasticsearch(connection_class=RequestsHttpConnection,
                   http_auth=(ELASTIC_USER, ELASTIC_PWD))

# In[4]:

es.indices.delete(index=ELASTIC_INDEX)

# In[5]:

# Create index and mapping
mapping = {
    "settings": {
        "index": {
            "store": {
                "type": "fs"
            },
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "analysis": {
            "analyzer": {
                "my_english": {
                    "type": "english",
                    "stopwords_path": "stoplist.txt"
                }
            }
        }
    },
    "mappings": {
        "document": {
            "_size": {
                "enabled": "true"
            },
            "properties": {
                "fileid": {
                    "type": "keyword",
                    "store": "true"
                },
                "doc_length": {
                    "type": "integer",
                    "store": "true"
                },
                "text": {
                    "type": "text",
                    "store": "true",
                    "term_vector": "with_positions_offsets_payloads",
                    "analyzer": "my_english",
                    "fielddata": "true"
                }
            }
        }
    }
}

es.indices.create(index=ELASTIC_INDEX, body=mapping)

# In[6]:

for root, _, files in os.walk("./IR_DATA/AP_DATA/ap89_collection", topdown=False):
    for name in files:
        if name.startswith("ap"):
            print(os.path.join(root, name))
            try:
                with open(os.path.join(root, name), encoding="utf-8") as fp:
                    soup = BeautifulSoup(fp, "lxml")
                    doc_docs = []
                    for d in soup.find_all("doc"):
                        doc_text = ""
                        # Add all the text elements in the docs to temp variable
                        for t in d.find_all("text"):
                            doc_text += t.text.strip() + " "

                        # Creating document to index
                        index_doc = {
                            '_type': ELASTIC_TYPE,
                            '_index': ELASTIC_INDEX,
                            '_id': d.docno.string.strip(),
                            'doc_length': len(
                                es.indices.analyze(index=ELASTIC_INDEX, analyzer="my_english", body={"text": doc_text})[
                                    "tokens"]),
                            'text': doc_text,
                            'fileid': d.fileid.string.strip(),
                            'timestamp': datetime.now()
                        }
                        # Add all documents in this file to a list for bulk import
                        doc_docs.append(index_doc)
                    helpers.bulk(es, doc_docs)
            except UnicodeDecodeError:
                print("Unicode Error: ", name)
es.indices.refresh(index=ELASTIC_INDEX)


# In[88]:




# In[93]:




# In[27]:

# with open("./IR_DATA/AP_DATA/ap89_collection/ap890520", encoding="utf-8") as fp:
#     soup = BeautifulSoup(fp, "lxml")
#     doc_docs = []
#     doc = 0
#     for d in soup.find_all("doc"):
#         doc += 1
#         for t in d.find_all("text"):
#             print(doc, t.text.strip())



# In[ ]:



