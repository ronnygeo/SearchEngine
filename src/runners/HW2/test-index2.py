import re
from collections import defaultdict

from src.commons.variables import output_folder
from src.indexer.IndexUtils import IndexUtils
from src.models.QueryLocal import Query
from src.models.ScoringLocal import Scoring
from src.query.LocalSearch import LocalSearch

search = LocalSearch("ap89test")
utils = IndexUtils(regex = r'[\s:,`*\\*/*!_*\.]')



# Load all the queries
queries = []
with open('../../input/AP_DATA/query_test.txt', 'r') as f:
    for line in f:
        print(line)
        reg_match = re.match(r'^(\d+).(.*)', line)
        queries.append(Query(reg_match.group(1).strip(), search, reg_match.group(2).strip(), 10000, "lancaster"))

# Initializing a scoring object
scoring = Scoring(search, queries)

# List of operations
vs_ops = ['tfidf', 'bm25']
lm_ops = ['laplace']

print("VSM Ops.")
# Doing all the vs ops and writing to a file
for op in vs_ops:
    scoring.processVS(op)
    scoring.write_file(op, output_folder + op + '.temp' )

print("LM Ops.")
# Doing all the lm ops and writing to a file
for op in lm_ops:
    scoring.processLM(op)
    scoring.write_file(op, output_folder + op + '.temp' )

print("proximity")
scoring.proximitySearch()
scoring.write_file("proximity", output_folder + "proximity.temp")