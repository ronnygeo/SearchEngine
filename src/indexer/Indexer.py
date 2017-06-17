from src.commons.variables import temp_index_file, output_folder, index_folder, misc_folder, temp_folder
from collections import defaultdict, OrderedDict
import os

class Indexer(object):
    def __init__(self, name, existing=False, cfile=None, tfile=None, dfile=None):
        self.termMap = []
        self.docMap = []
        self.indexname = name
        self.catalog = defaultdict(int)
        self.df = defaultdict(int)
        self.ttf = defaultdict(int)
        self.dlen = defaultdict(int)
        self.temp_file = temp_folder + name + "temp" + ".index"
        self.temp_catalog_file = temp_folder + name + "temp" + ".catalog"
        self.clearTempFile()
        self.consolidate = 0
        self.catalog2 = {}

        if existing:
            self.readDocMap(docFile=dfile)
            self.readTermMap(tfile)
            self.readCatalog(cfile)

    def clearTempFile(self):
        try:
            f = open(self.temp_file, 'r+')
            f.truncate()
        except FileNotFoundError:
            f = open(self.temp_file, 'w')

    def readTermMap(self, termFile):
        termMap = []
        # Read from file
        with open(termFile, "r") as infile:
            for line in infile:
                termMap.append(line.strip())
        self.termMap = termMap

    def readDocMap(self, docFile):
        docMap = []
        # Read from file
        with open(docFile, "r") as infile:
            for line in infile:
                docMap.append(line.strip())
        self.docMap = docMap

    def getVocabSize(self):
        return len(self.termMap)

    def getSumTTF(self):
        return len(self.ttf.values())

    def getDLen(self, name):
        id = self.getItemName(name, self.docMap)
        return self.dlen[id]

    def addDLen(self, name, dlen):
        id = self.getSetItemId(name, self.docMap)
        self.dlen[id] = dlen

    def getAvgDLen(self):
        return sum(self.dlen.values())/len(self.dlen)

    def getSetItemId(self, term, someMap):
        if term not in someMap:
            i = len(someMap)
            someMap.append(term)
        else:
            i = someMap.index(term)
        return i

    def getItemName(self, id, someMap):
        return someMap[id]

    def createTermList(self, docs_tokens):
        itermCount = defaultdict(dict)
        for doc, tokens in docs_tokens.items():
            for token in enumerate(tokens):
                pos = token[0]
                tokenId = self.getSetItemId(token[1], self.termMap)
                docId = self.getSetItemId(doc, self.docMap)

                if tokenId not in itermCount.keys():
                    itermCount[tokenId] = {}
                if docId not in itermCount[tokenId].keys():
                    itermCount[tokenId][docId] = {}
                    itermCount[tokenId][docId]['tf'] = 0
                    itermCount[tokenId][docId]['pos'] = []
                itermCount[tokenId][docId]['tf'] += 1
                itermCount[tokenId][docId]['pos'].append(pos)
        for tokenId, docs in itermCount.items():
            self.df[tokenId] += len(docs.keys())
            self.ttf[tokenId] += sum(map(lambda d: d['tf'], docs.values()))

        for tokenId in itermCount.keys():
            temp_docs = itermCount[tokenId]
            sorted_docs = OrderedDict(sorted(temp_docs.items(), key=lambda x: x[1]["tf"], reverse=True))
            itermCount[tokenId] = sorted_docs
        return itermCount

    def readTermFromFile(self, filename, termIdRef):
        ntc = {}
        f = open(filename)
        f.seek(self.catalog[termIdRef])
        line = f.readline().strip()
        (termId, df, ttf, docs) = line.strip().split(":")
        mdocs = docs.split("|")
        for p in mdocs:
            docId, tf, els = p.split(",")
            ntc[docId] = {}
            ntc[docId]['tf'] = int(tf)
            ntc[docId]['pos'] = list(map(lambda x: int(x), els.split(".")))
        return ntc, int(df), int(ttf)

    def readTermFromFile2(self, filename, termIdRef):
        ntc = {}
        f = open(filename)
        f.seek(self.catalog2[termIdRef][filename])
        line = f.readline().strip()
        (termId, df, ttf, docs) = line.strip().split(":")
        mdocs = docs.split("|")
        for p in mdocs:
            docId, tf, els = p.split(",")
            ntc[docId] = {}
            ntc[docId]['tf'] = int(tf)
            ntc[docId]['pos'] = list(map(lambda x: int(x), els.split(".")))
        return ntc, int(df), int(ttf)

    def mergeDocs(self, termId, filename, nw_docs):
        old_docs, df, ttf = self.readTermFromFile(filename, termId)

        join_docs = {}
        join_docs.update(nw_docs)
        join_docs.update(old_docs)
        newdocs = OrderedDict(sorted(join_docs.items(), key=lambda x: x[1]["tf"], reverse=True))
        return newdocs

    def writeTempData(self, itermCount, consolidateRate=5):
        with open(self.temp_file, "a") as out:
            for termId, docs in itermCount.items():
                if termId in self.catalog.keys():
                    # print("Merging {} term with previous index.".format(termId))
                    docs = self.mergeDocs(termId, self.temp_file, docs)
                    # print(docs)
                self.catalog[termId] = out.tell()
                outStr = str(termId) + ":" + str(self.df[termId]) + ":" + str(self.ttf[termId]) + ":"
                tempStr = []

                for docId, values in docs.items():
                    tempStr.append(str(docId) + "," + str(values['tf']) + "," + "{}".format(
                        ".".join(map(lambda x: str(x), values['pos']))))
                outStr += "|".join(tempStr)
                # print(outStr)
                out.write(outStr + "\n")
        self.consolidate += 1
        if self.consolidate > consolidateRate:
            print("Consolidating temp index. Current vocab size: ", len(self.termMap))
            self.consolidate_temp()
            self.consolidate = 0

    def writeCatalog(self, filename):
        with open(filename, "w+") as out:
            for termId, offset in self.catalog.items():
                out.write(str(termId) + "," + str(offset) + "\n")

    def writeCatalogTemp(self, filename, indexfile):
        with open(filename, "w+") as out:
            for termId, offset in self.catalog2[indexfile].items():
                out.write(str(termId) + "," + str(offset) + "\n")

    def writeTermMap(self, filename):
        with open(filename, "w+") as out:
            for term in self.termMap:
                out.write(term + "\n")

    def writeDocMap(self, filename):
        with open(filename, "w+") as out:
            for doc in self.docMap:
                out.write(doc + "\n")

    def readCatalog(self, catalogFile):
        catalog = defaultdict(int)
        # Read from file
        with open(catalogFile, "r") as infile:
            for line in infile:
                termId, offset = line.strip().split(",")
                catalog[int(termId)] = int(offset)
        self.catalog = catalog

    def writeDLen(self, filename):
        with open(filename, "w+") as out:
            for termId, dlen in self.dlen.items():
                out.write(str(termId) + "," + str(dlen) + "\n")

    def writeTTF(self, filename):
        with open(filename, "w+") as out:
            for termId, dlen in self.ttf.items():
                out.write(str(termId) + "," + str(dlen) + "\n")

    def writeDF(self, filename):
        with open(filename, "w+") as out:
            for termId, dlen in self.df.items():
                out.write(str(termId) + "," + str(dlen) + "\n")

    def readDLen(self, dlenFile):
        catalog = defaultdict(int)
        # Read from file
        with open(dlenFile, "r") as infile:
            for line in infile:
                termId, dlen = line.strip().split(",")
                catalog[int(termId)] = int(dlen)
        self.dlen = catalog

    def readTTF(self, dlenFile):
        catalog = defaultdict(int)
        # Read from file
        with open(dlenFile, "r") as infile:
            for line in infile:
                termId, dlen = line.strip().split(",")
                catalog[int(termId)] = int(dlen)
        self.ttf = catalog

    def readDF(self, dlenFile):
        catalog = defaultdict(int)
        # Read from file
        with open(dlenFile, "r") as infile:
            for line in infile:
                termId, dlen = line.strip().split(",")
                catalog[int(termId)] = int(dlen)
        self.df = catalog

    def reindex(self, outfile= output_folder + "1.index"):
        """ Reads from current temp file and catalog and creates a final index and catalog """
        new_catalog = defaultdict(int)
        with open(outfile, "w") as out:
            for termId, offset in self.catalog.items():
                docs, df, ttf = self.readTermFromFile(self.temp_file, termId)
                new_catalog[termId] = out.tell()
                outStr = str(termId) + ":" + str(df) + ":" + str(ttf) + ":"
                tempStr = []

                for docId, values in docs.items():
                    tempStr.append(str(docId) + "," + str(values['tf']) + "," + "{}".format(
                        ".".join(map(lambda x: str(x), values['pos']))))
                outStr += "|".join(tempStr)
                # print(outStr)
                out.write(outStr + "\n")
        self.catalog = new_catalog

    def consolidate_temp(self):
        """ Reads from current temp file and catalog and creates a final index and catalog """
        new_catalog = defaultdict(int)
        temp_index_file_new = self.temp_file+".new"
        with open(temp_index_file_new, "w") as out:
            for termId, offset in self.catalog.items():
                docs, df, ttf = self.readTermFromFile(self.temp_file, termId)
                new_catalog[termId] = out.tell()
                outStr = str(termId) + ":" + str(df) + ":" + str(ttf) + ":"
                tempStr = []

                for docId, values in docs.items():
                    tempStr.append(str(docId) + "," + str(values['tf']) + "," + "{}".format(
                        ".".join(map(lambda x: str(x), values['pos']))))
                outStr += "|".join(tempStr)
                out.write(outStr + "\n")
        os.remove(self.temp_file)
        os.rename(temp_index_file_new, self.temp_file)
        self.catalog = new_catalog

    def cleanup(self):
        index_file = index_folder + self.indexname + ".index"
        catalog_file = misc_folder + self.indexname + ".catalog"
        term_file = misc_folder + self.indexname + ".termmap"
        doc_file = misc_folder + self.indexname + ".docmap"
        dlen_file = misc_folder + self.indexname + ".dlen"
        ttf_file = misc_folder + self.indexname + ".ttf"
        df_file = misc_folder + self.indexname + ".df"

        self.reindex(index_file)
        self.writeCatalog(catalog_file)
        self.writeTermMap(term_file)
        self.writeDocMap(doc_file)
        self.writeDLen(dlen_file)
        self.writeTTF(ttf_file)
        self.writeDF(df_file)

        # self.clearTempFile()


    def mergeDocs2(self, termId, filename, nw_docs):
        old_docs, _, _ = self.readTermFromFile2(filename, termId)

        join_docs = {}
        join_docs.update(nw_docs)
        join_docs.update(old_docs)
        # TODO: Switch to custom merge sort
        newdocs = OrderedDict(sorted(join_docs.items(), key=lambda x: x[1]["tf"], reverse=True))
        return newdocs

    def writeTempData2(self, itermCount):
        current_file =  self.temp_file + str(self.consolidate)
        with open(current_file, "w+") as out:
            for termId, docs in itermCount.items():
                if termId not in self.catalog2.keys():
                    self.catalog2[termId] = defaultdict(int)
                offset = out.tell()
                self.catalog2[termId][current_file] = offset
                self.catalog[termId] = offset
                outStr = str(termId) + ":" + str(self.df[termId]) + ":" + str(self.ttf[termId]) + ":"
                tempStr = []

                for docId, values in docs.items():
                    tempStr.append(str(docId) + "," + str(values['tf']) + "," + "{}".format(
                        ".".join(map(lambda x: str(x), values['pos']))))
                outStr += "|".join(tempStr)
                # print(outStr)
                out.write(outStr + "\n")
        self.writeCatalog(self.temp_catalog_file + str(self.consolidate))
        self.catalog = defaultdict(int)
        self.consolidate += 1

    def mergeAll(self, outfile= output_folder + "1.index"):
        print("Writing final file..")
        new_catalog = defaultdict(int)
        with open(outfile, "w") as out:
            for termId, values in self.catalog2.items():
                docs = {}
                for filename, offset in values.items():
                    docs = self.mergeDocs2(termId, filename, docs)
                new_catalog[termId] = out.tell()
                outStr = str(termId) + ":" + str(self.df[termId]) + ":" + str(self.ttf[termId]) + ":"
                tempStr = []

                for docId, values in docs.items():
                    tempStr.append(str(docId) + "," + str(values['tf']) + "," + "{}".format(
                        ".".join(map(lambda x: str(x), values['pos']))))
                outStr += "|".join(tempStr)
                # print(outStr)
                out.write(outStr + "\n")
        self.catalog = new_catalog

    def cleanup2(self):
        index_file = index_folder + self.indexname + ".index"
        catalog_file = misc_folder + self.indexname + ".catalog"
        term_file = misc_folder + self.indexname + ".termmap"
        doc_file = misc_folder + self.indexname + ".docmap"
        dlen_file = misc_folder + self.indexname + ".dlen"
        ttf_file = misc_folder + self.indexname + ".ttf"
        df_file = misc_folder + self.indexname + ".df"

        self.mergeAll(index_file)
        self.writeCatalog(catalog_file)
        self.writeTermMap(term_file)
        self.writeDocMap(doc_file)
        self.writeDLen(dlen_file)
        self.writeTTF(ttf_file)
        self.writeDF(df_file)

        # self.clearTempFile()