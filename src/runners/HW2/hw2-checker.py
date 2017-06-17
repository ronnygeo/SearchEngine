import re
from collections import defaultdict

from src.commons.variables import output_folder
from src.indexer.IndexUtils import IndexUtils
from src.models.QueryLocal import Query
from src.models.ScoringLocal import Scoring
from src.query.LocalSearch import LocalSearch

search = LocalSearch("ap89p3")
utils = IndexUtils()

# Load all the queries
terms = []
with open('../../input/hw2/checklist.txt', 'r') as f:
    for line in f:
        # terms.append(line.strip())
        terms.append(utils.preprocess(line.strip())[0])

with open('../../output/hw2/checklist.txt', 'w') as f:
    for term in terms:
        search_res = search.searchTerm(term)
        if search_res:
            _, df, ttf = search_res
        else:
            df = 0
            ttf = 0
        f.write("{} {} {}\n".format(term, df, ttf))
