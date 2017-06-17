from collections import defaultdict, OrderedDict
from src.commons.variables import output_folder, index_folder, misc_folder

class LocalSearch(object):
    def __init__(self, indexname):
        self.termMap = []
        self.docMap = []
        self.catalog = defaultdict(int)
        self.ttf = defaultdict(int)
        self.df = defaultdict(int)

        self.readDocMap(docFile=misc_folder+indexname+".docmap")
        self.readTermMap(misc_folder+indexname+".termmap")
        self.readCatalog(misc_folder+indexname+".catalog")
        self.indexFile = index_folder+indexname+".index"
        self.readDLen(misc_folder+indexname+".dlen")
        self.readTTF(misc_folder + indexname + ".ttf")
        self.readDF(misc_folder + indexname + ".df")

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

    def getVocabSize(self):
        return len(self.termMap)

    def getNumDocs(self):
        return len(self.docMap)

    def getSumTTF(self):
        return sum(self.ttf.values())

    def getItemName(self, id, someMap):
        return someMap[id]

    def getItemId(self, term, someMap):
        if term not in someMap:
            return None
        else:
            i = someMap.index(term)
        return i

    def readCatalog(self, catalogFile):
        catalog = defaultdict(int)
        # Read from file
        with open(catalogFile, "r") as infile:
            for line in infile:
                termId, offset = line.strip().split(",")
                catalog[int(termId)] = int(offset)
        self.catalog = catalog

    def readDLen(self, dlenFile):
        catalog = defaultdict(int)
        # Read from file
        with open(dlenFile, "r") as infile:
            for line in infile:
                termId, dlen = line.strip().split(",")
                catalog[int(termId)] = int(dlen)
        self.dlen = catalog

    def getDLen(self, name):
        id = self.getItemId(name, self.docMap)
        return self.dlen[id]

    def getAvgDLen(self):
        return sum(self.dlen.values())/len(self.dlen)

    def searchTerm(self, term):
        docs = {}
        termIdRef = self.getItemId(term, self.termMap)
        # print(self.catalog[termIdRef])
        if termIdRef is not None:
            f = open(self.indexFile)
            f.seek(self.catalog[termIdRef])
            line = f.readline().strip()
            # print(line)
            (termId, df, ttf, sdocs) = line.strip().split(":")
            mdocs = sdocs.split("|")
            for p in mdocs:
                docId, tf, els = p.split(",")
                doc = self.getItemName(int(docId), self.docMap)
                docs[doc] = {}
                docs[doc]['tf'] = int(tf)
                docs[doc]['pos'] = list(map(lambda x: int(x), els.split(".")))
            return docs, int(df), int(ttf)
        else:
            return None

    def getDF(self, word):
        id = self.getItemId(word, self.termMap)
        return self.df[id]