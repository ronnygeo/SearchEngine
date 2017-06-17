from src.indexer.IndexUtils import IndexUtils
from src.indexer.Indexer import Indexer

# Test data

txt = "Hello SDw:sd, and the ftf'' bvg\" hjh. uhnj. in on //sd \wesds not asda d. earlier; earlier;: earlier: _ _ewew adf! 989.09 6:09 adadsa-adas."
txt2 = "Hello SDw:sd, asda d. adf! 989.09 6:09 adadsa-adas.Hello SDw:sd, asda d. adf! 989.09 6:09 adadsa-adas."
txt3 = "Hello ``interkal ``` SDw:sd, $230 (6786) ```Ineedthis ```the ````for asda d. adf! 989.adas.Hello SDw:sd, asda d. adf! 989.09 6:09 adadsa-adas."
txt4 = "Hello SDw:sd, asda d. a"
txt5 = "Hello SDw:sd, asda d. adf! 989.09 6:09 adadsa-adas.Hello SDw:sd, asda d. adf! 989.09 6:09 adadsa-adas.Hello SDw:sd, asda d. abs iojknas sdfdsd"
txt10 = "Hello SDw:sd, asda d. adf! 9df frfw3 as 1 -adas.Hello SDw:sd, asda d. adf! 989.09 6:09 adadsa-adas.Hello SDw:sd, asda d. abs"

batch_docs = [{"d1": {"text": txt, "len": 1}, "d2": {"text": txt2, "len": 1}, "d3": {"text": txt3, "len": 1}, "d4": {"text": txt4, "len": 1}, "d5": {"text": txt5, "len": 1}},
        {"d6": {"text": txt, "len": 1}, "d7": {"text": txt2, "len": 1}, "d8": {"text": txt3, "len": 1}, "d9": {"text": txt4, "len": 1}, "d10": {"text": txt10, "len": 1}}]


batch_size = 1000
indexer = Indexer(name="test")
utils = IndexUtils()
count = 0

for docs in batch_docs:
    count += 1
    doc_tokens = {}
    for doc in docs:
        indexer.addDLen(doc, docs[doc]["len"])
        doc_tokens[doc] = utils.preprocess(docs[doc]["text"])
    itermCount = indexer.createTermList(doc_tokens)
    indexer.writeTempData2(itermCount)
    print("Doc ", count, " Done")

indexer.cleanup2()
