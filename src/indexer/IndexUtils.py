import re
from nltk.stem.lancaster import LancasterStemmer
from nltk.stem.porter import PorterStemmer
from src.commons.variables import stopwords_file


class IndexUtils:
    def __init__(self, regex = r"[\s:,`*\\*/*!_*'*]"):
        self.regex = regex
        self.sws = self.initializeSW()
        self.stemm = "porter"

    def initializeSW(self):
        sw = []
        with open(stopwords_file, "r") as infile:
            for line in infile:
                sw.append(line.strip())
        return sw

    def tokenizer(self, text):
        return list(filter(lambda x: x != "", re.compile(self.regex).split(text.lower())))

    def tokenizer2(self, text):
        new_tokens = re.sub(r"[\s\t\r\n:,`\\/!_'();\"\[\]\{\}@$#&?~]", " ", text.lower())
        nt_tokens = re.compile("\s").split(re.sub(r"[-*@]", "", new_tokens))
        ntokens = []
        for token in nt_tokens:
            if token != "":
                if token[len(token)-1] == ".":
                    ntokens.append(token[:-1])
                else:
                    ntokens.append(token)
        return ntokens


    def stemmer(self, term):
        if self.stemm == "lancaster":
            stemmer = LancasterStemmer()
        else:
            stemmer = PorterStemmer()
        return stemmer.stem(term)

    def stopwords_removal(self, tokens):
        new_tokens = []
        for token in tokens:
            if token not in self.sws:
                new_tokens.append(token)
        return new_tokens

    def preprocess(self, text, stem=True, stemmer="porter"):
        tokens = self.tokenizer2(text)
        self.stemm = stemmer
        swtokens = self.stopwords_removal(tokens)
        if stem:
            return list(map(self.stemmer, swtokens))
        return swtokens
