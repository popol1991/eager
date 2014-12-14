import sys
import time
from elasticsearch import Elasticsearch
from search import search_table

SEARCH_TYPES = ["best_fields", "cross_fields", "phrase"]

if __name__ == '__main__':
    """
    For each of a list of queries, attach the number of retrieved documents
    for each search type in SEARCH_TYPES
    """
    es = Elasticsearch([
        {'host': 'localhost', 'port': 14010}
    ])

    print ", ".join(["Query"] + SEARCH_TYPES)
    count = 0
    begin = time.time()
    for query in sys.stdin:
        count += 1
        if count % 100 == 0:
            print >> sys.stderr, count, time.time()-begin, "secs"
        query = query.strip()
        results = []
        for stype in SEARCH_TYPES:
            results.append(str(search_table(es, query.split(' '), stype)))
        print ", ".join([query] + results)
