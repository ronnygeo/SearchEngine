from collections import defaultdict

from bs4 import BeautifulSoup
import re
import os

from bs4.diagnose import diagnose
from elasticsearch import Elasticsearch, helpers
from datetime import datetime


ELASTIC_INDEX = "elastic4"
ELASTIC_TYPE = "document"

from elasticsearch import Elasticsearch, RequestsHttpConnection

# es = Elasticsearch(
#     hosts=[{'host': '172.20.10.4', 'port': 9200}]
#     ,
#     connection_class=RequestsHttpConnection
# )
#
# doc_count = 0
#
# # es.indices.delete(index=".kibana")
# es.indices.delete(index=ELASTIC_INDEX)
#
#
# # Create index and mapping
# mapping = {
#     "settings": {
#         "index": {
#             "store": {
#                 "type": "fs"
#             },
#             "number_of_shards": 4,
#             "number_of_replicas": 0
#         },
#         "analysis": {
#             "analyzer": {
#                 "my_english": {
#                     "type": "english",
#                     "stopwords_path": "stoplist.txt"
#                 }
#             }
#         }
#     },
#     "mappings": {
#         "document": {
#             "properties": {
#                 "docno": {
#                     "type": "text",
#                     "store": "true",
#                     "index": "analyzed",
#                     "term_vector": "with_positions_offsets_payloads"
#                     },
#                 "HTTPheader": {
#                     "type": "text",
#                     "store": "true",
#                     "index": "not_analyzed"
#                     },
#                 "title": {
#                     "type": "text",
#                     "store": "true",
#                     "index": "analyzed",
#                     "term_vector": "with_positions_offsets_payloads"
#                     },
#                 "text": {
#                     "type": "text",
#                     "store": "true",
#                     "index": "analyzed",
#                     "term_vector": "with_positions_offsets_payloads"
#                 },
#                 "html_Source": {
#                     "type":"text",
#                       "store": "true",
#                       "index": "no"
#                 },
#                 "in_links": {
#                     "type": "text",
#                     "store": "true",
#                     "index": "no"
#             },
#                 "out_links": {
#                     "type": "text",
#                     "store": "true",
#                     "index": "no"
#             },
#                 "author": {
#                      "type": "keyword",
#                     "store": "true",
#                     "index": "analyzed"
#             },
#                 "depth": {
#                     "type": "integer",
#                     "store": "true"
#                 },
#                 "url": {
#                     "type": "keyword",
#                     "store": "true"
#                 }
#             }
#          }
#     }
# }
#
# es.indices.create(index=ELASTIC_INDEX, body=mapping)
#
# print("Created Index.")

# Load inlinks to memory
# inlinks = defaultdict(set)

# with open("../../../output/hw3/inlinks5.map", "rb") as infile:
#     for line in infile:
#         all_urls = line.decode("UTF-8").split(" ")
#         url = all_urls[0]
#         inurls = all_urls[1:]
#         inlinks[url] = map(lambda t: t.strip(), inurls)

doc_count = 0
for root, _, files in os.walk("../../../output/hw3/", topdown=False):
# for root, _, files in os.walk("../../../output/datafix/", topdown=False):
    for name in files:
        if name.startswith("test"):
        # if name.startswith("data"):
            print(os.path.join(root, name))
            # try:
            with open(os.path.join(root, name), encoding="utf-8") as fp:
                diagnose(fp.read())
                soup = BeautifulSoup(fp.read(), "html.parser")
                doc_docs = []
                # try:
                soup_docs = soup.find_all("doc", recursive=False)
                for d in soup_docs:
                    print(d.docno.text)
                    docid = d.docno.text.strip()
                    doc_text = d.content.text.strip()
                    # inlink = []
                    # if docid in inlinks.keys():
                    #     inlink = inlinks[docid]
                    outlink = []
                    print(name, docid)
                    if d.outlinks:
                        outlink = d.outlinks.text.strip().split("\t")
                    # Creating document to index
                    index_doc = {
                        '_type': ELASTIC_TYPE,
                        '_index': ELASTIC_INDEX,
                        '_id': docid,
                        'HEAD': d.title.text,
                        'depth': int(d.depth.text),
                        'HTTP_header': d.httpheader.text,
                        'html_Source': d.HTMLSOURCE.text,
                        'out_links': outlink,
                        # 'in_links': list(inlink),
                        'url': docid,
                        # 'doc_length': len(
                        #     es.indices.analyze(index=ELASTIC_INDEX, analyzer="my_english", body={"text": doc_text})[
                        #         "tokens"]),
                        'text': doc_text,
                        'author': "Ronny Mathew",
                        'timestamp': datetime.now()
                    }
                    # Add all documents in this file to a list for bulk import
                    doc_docs.append(index_doc)
                    doc_count += 1
                    print("added doc: ", doc_count)
                # except Exception:
                #     print("Got an exception.")
                #     print(soup)

                    # helpers.bulk(es, doc_docs)
            # except UnicodeDecodeError:
            #     print("Unicode Error: ", name)
# es.indices.refresh(index=ELASTIC_INDEX)
