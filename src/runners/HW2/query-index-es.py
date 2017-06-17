import re
from src.helpers.ESHelpers import ESHelpers
from src.models.Scoring import Query, Scoring

es_helper = ESHelpers()

# Load all the queries
queries = []
with open('../../input/AP_DATA/query_desc.51-100.short.txt', 'r') as f:
    for line in f:
        reg_match = re.match(r'^(\d+).(.*)', line)
        tokens = es_helper.get_tokens(reg_match.group(2).strip())
        queries.append(Query(reg_match.group(1).strip(), es_helper, tokens))

# In[54]:

# Initializing a scoring object
scoring = Scoring(queries)

# In[55]:

# List of operations
vs_ops = ['okapi', 'tfidf', 'bm25']
lm_ops = ['laplace', 'jmercer']

# In[56]:

# Doing all the vs ops and writing to a file
for op in vs_ops:
    scoring.processVS(op)
# scoring.write_file( op, op + '.1000' )


# In[ ]:

# Doing all the lm ops and writing to a file
for op in lm_ops:
    scoring.processLM(op, 0.95)
# scoring.write_file( op, op + '.1000' )


# In[29]:

scoring.normalize()

# In[32]:

scoring.write_meta_file("meta.1000")

