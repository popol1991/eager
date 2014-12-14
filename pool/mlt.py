import sys
import json
from elasticsearch import Elasticsearch

INDEX = "table"
TABLE = "table"

TOPK = 10
PERCENT_TERMS_TO_MATCH = 0.2
ALL_FIELDS = ["caption", "footnote_*", "heading_*", "citation_*", "header_rows.header_*", "data_rows.svalue_0"]

def mlt_query(ids, fields=ALL_FIELDS, size=10, percent=PERCENT_TERMS_TO_MATCH):
    query = {
        "query": {
            "more_like_this": {
                "fields": fields,
                "ids": ids,
                "percent_terms_to_match": percent,
                "include": False
            }
        }
    }
    return query

def search(es, query):
    return es.search(index=INDEX, doc_type=TABLE, body=query)

if __name__ == '__main__':
    if not len(sys.argv) in [3,4]:
        print "Usage: python mlt.py data option [matched]"
        exit(1)

    es = Elasticsearch([
        {"host":"compute-2-15", "port": 14010}
    ])

    with open(sys.argv[1]) as fin:
       hits = json.load(fin)["responses"][0]["hits"]

    option = sys.argv[2]
    if option == 'expand':
        ids = [hit["_id"] for hit in hits["hits"]][:TOPK]
        query = mlt_query(ids, ALL_FIELDS)
        res = search(es, query)
        print res["hits"]["hits"]
    elif option == 'single':
        results = []
        res_ids = set()
        for i in range(min(TOPK, len(hits))):
            hit = hits["hits"][i]
            ids = [hit["_id"]]
            fields = hit["highlight"].keys() if len(sys.argv) == 4 else ALL_FIELDS
            query = mlt_query(ids, fields)
            res = search(es, query)
            for t in res["hits"]["hits"]:
                if not t["_id"] in res_ids:
                    results.append(t)
                    res_ids.add(t["_id"])
        print results
        print
        print len(results)
    else:
        print "wrong option."
