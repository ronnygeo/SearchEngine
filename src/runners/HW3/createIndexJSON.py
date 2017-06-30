import ujson
import re
import os
from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection
from datetime import datetime


ELASTIC_INDEX = "elastic4"
ELASTIC_TYPE = "document"

es = Elasticsearch(
    hosts=[{'host': '10.6.9.75', 'port': 9200}]
    ,
    connection_class=RequestsHttpConnection
)

doc_count = 0

# es.indices.delete(index=".kibana")
es.indices.delete(index=ELASTIC_INDEX)


# Create index and mapping
mapping = {
    "settings": {
        "index": {
            "store": {
                "type": "fs"
            },
            "number_of_shards": 4,
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
            "properties": {
                "docno": {
                    "type": "text",
                    "store": "true",
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads"
                    },
                "HTTPheader": {
                    "type": "text",
                    "store": "true",
                    "index": "not_analyzed"
                    },
                "title": {
                    "type": "text",
                    "store": "true",
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads"
                    },
                "text": {
                    "type": "text",
                    "store": "true",
                    "index": "analyzed",
                    "term_vector": "with_positions_offsets_payloads"
                },
                "html_Source": {
                    "type":"text",
                      "store": "true",
                      "index": "no"
                },
                "in_links": {
                    "type": "text",
                    "store": "true",
                    "index": "no"
            },
                "out_links": {
                    "type": "text",
                    "store": "true",
                    "index": "no"
            },
                "author": {
                     "type": "keyword",
                    "store": "true",
                    "index": "analyzed"
            },
                "depth": {
                    "type": "integer",
                    "store": "true"
                },
                "url": {
                    "type": "keyword",
                    "store": "true"
                }
            }
         }
    }
}

es.indices.create(index=ELASTIC_INDEX, body=mapping)

print("Created Index.")

# Load inlinks to memory
with open("../../../output/hw3/inlinksj20.map", "rb") as infile:
        inlinks = ujson.loads(infile.readline())

doc_count = 0
# for root, _, files in os.walk("../../../output/hw3/", topdown=False):
for root, _, files in os.walk("../../../output/dataj20/", topdown=False):
    for name in files:
        # if name.startswith("test"):
        if name.startswith("data"):
            print(os.path.join(root, name))
            with open(os.path.join(root, name), encoding="UTF-8") as fp:
                doc_docs = []
                for line in fp:
                    data = ujson.loads(line)
                    docid = data["url"].strip()
                    doc_text = data["content"].strip()
                    inlink = []
                    if docid in inlinks.keys():
                        inlink = inlinks[docid]
                    outlink = []
                    print(name, docid)
                    if data["outlinks"]:
                        outlink = data["outlinks"]
                    # Creating document to index
                    index_doc = {
                        '_type': ELASTIC_TYPE,
                        '_index': ELASTIC_INDEX,
                        '_id': docid,
                        'HEAD': data["title"],
                        'depth': int(data["depth"]),
                        'HTTP_header': data["httpheader"],
                        'html_Source': data["htmlsource"],
                        'out_links': outlink,
                        'in_links': list(inlink),
                        'url': docid,
                        'doc_length': len(
                            es.indices.analyze(index=ELASTIC_INDEX, analyzer="my_english", body={"text": doc_text})[
                                "tokens"]),
                        'text': doc_text,
                        'author': "Ronny Mathew",
                        'timestamp': datetime.now()
                    }
                    # Add all documents in this file to a list for bulk import
                    doc_docs.append(index_doc)
                    doc_count += 1
                    print("added doc: ", doc_count)
                helpers.bulk(es, doc_docs)
es.indices.refresh(index=ELASTIC_INDEX)
