from src.indexer.IndexUtils import IndexUtils
from src.indexer.Indexer import Indexer
import os
from bs4 import BeautifulSoup


batch_size = 800
indexer = Indexer("ap89p3")
utils = IndexUtils()

count = 0

def index(doc_batch, count=0):
    # print("Running batch indexing for {} docs.".format(len(doc_batch)))
    doc_tokens = {}
    for doc in doc_batch:
        doc_tokens[doc] = utils.preprocess(doc_batch[doc]['text'])
        indexer.addDLen(doc, len(doc_tokens[doc]))
    itermCount = indexer.createTermList(doc_tokens)
    indexer.writeTempData2(itermCount)
    print("Completed batch indexing for {} docs.".format(len(doc_batch)))
    count += len(doc_batch)
    print("Processed {} docs.".format(count))


for root, _, files in os.walk("../../input/AP_DATA/ap89_collection", topdown=False):
    doc_batch = {}
    for name in files:
        if name.startswith("ap"):
            print(os.path.join(root, name))
            try:
                with open(os.path.join(root, name), encoding="utf-8") as fp:
                    soup = BeautifulSoup(fp, "html5lib")
                    for d in soup.find_all("doc"):
                        doc_text = ""
                        # Add all the text elements in the docs to temp variable
                        for t in d.find_all("text"):
                            doc_text += t.text.strip() + " "
                        doc_id = d.docno.string.strip()

                        # Creating document to index
                        doc_batch[doc_id] = {"text": doc_text}

            except UnicodeDecodeError:
                print("Unicode Error: ", name)

        if len(doc_batch.keys()) > batch_size:
            index(doc_batch, count)
            doc_batch = {}

    if len(doc_batch.keys()) > 0:
        index(doc_batch, count)
        doc_batch = {}

indexer.cleanup2()


