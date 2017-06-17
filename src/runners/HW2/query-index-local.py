import re
from collections import defaultdict

from src.commons.variables import output_folder
from src.indexer.IndexUtils import IndexUtils
from src.models.QueryLocal import Query
from src.models.ScoringLocal import Scoring
from src.query.LocalSearch import LocalSearch

regex = r'[\s:,`*\\*/*!_*]'
stemmer = "porter"
indexname = "ap89p3"

search = LocalSearch(indexname)
utils = IndexUtils(regex = regex)

# Load all the queries
queries = []
# with open('../../input/AP_DATA/query_test.txt', 'r') as f:
with open('../../input/AP_DATA/query_desc.51-100.short.txt', 'r') as f:
    for line in f:
        print(line)
        reg_match = re.match(r'^(\d+).(.*)', line)
        queries.append(Query(reg_match.group(1).strip(), search, reg_match.group(2).strip(), 7000, stemmer))

# Initializing a scoring object
scoring = Scoring(search, queries)

print("Running VSM models..")
# List of operations
vs_ops = ['tfidf', 'bm25']
lm_ops = ['laplace']

# Doing all the vs ops and writing to a file
for op in vs_ops:
    scoring.processVS(op)
    scoring.write_file(op, output_folder + op + '.1000')

print("Running LM models..")
# Doing all the lm ops and writing to a file
for op in lm_ops:
    scoring.processLM(op)
    scoring.write_file(op, output_folder + op + '.1000')

print("Running proximity search..")
scoring.proximitySearchCombined()
scoring.write_file("proximityCombined", output_folder + "proximity.1000")
