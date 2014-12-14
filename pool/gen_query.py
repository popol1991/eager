import json
import sys

head = '{"index":"table","type":"table","size":50,"from":0}'
FIELDS = ["caption", "footnote_*", "heading_*", "citation_*", "header_rows.header_*", "data_rows.svalue_0"]

SINGLE_WEIGHT = 8
PHRASE_WEIGHT = 1
UW_WEIGHT = 1

def sdm_query(terms, size=10):
    global FIELDS
    global SINGLE_WEIGHT
    clauses = []
    for term in terms:
        query = {
            "multi_match": {
                "query": term,
                "fields": FIELDS,
                "boost": SINGLE_WEIGHT
            }
        }
        clauses.append(query)
    # phrase query
    for i in range(len(terms)-1):
        phrase_query = {
            "multi_match": {
                "query": " ".join([terms[i], terms[i+1]]),
                "fields": FIELDS,
                "type": "phrase",
                "boost": PHRASE_WEIGHT
            }
        }
        clauses.append(phrase_query)
    # unordered window query
    for field in FIELDS:
        for i in range(len(terms)-1):
            uw_query = {
                "span_near": {
                    "clauses": [
                        {"span_term": {field: terms[i]}},
                        {"span_term": {field: terms[i+1]}}
                    ],
                    "slop": 8,
                    "in_order": False,
                    "boost": UW_WEIGHT
                }
            }
            clauses.append(uw_query)
    query = {"query":{"bool":{"should":clauses}}, "size":size}
    return json.dumps(query, separators=(',',':'))

def match_query(terms, size=10):
    query = {
            "query":{"multi_match":{"query":' '.join(terms),"type":"cross_fields","fields":["caption","footnote_*","heading_*","citation_*","header_rows.header_*","data_rows.svalue_0"]}},
            "highlight":{"fields": {"header_rows.header_*":{},"data_rows.svalue_0":{},"caption":{},"footnote_*":{},"heading_*":{},"citation_*":{}}},
            "size": size
            }
    return json.dumps(query, separators=(',',':'))

if __name__ == '__main__':
    with open(sys.argv[1]) as fin:
        if sys.argv[2] == 'bow':
            for line in fin:
                query = line.strip().split(':')[1]
                print head
                print '{"query":{"multi_match":{"query":"%s","type":"cross_fields","fields":["caption","footnote_*","heading_*","citation_*","header_rows.header_*","data_rows.svalue_0"]}},"highlight":{"fields": {"header_rows.header_*":{},"data_rows.svalue_0":{},"caption":{},"footnote_*":{},"heading_*":{},"citation_*":{}}}}' % query
        elif sys.argv[2] == 'sdm':
            for line in fin:
                query = line.strip().split(':')[1]
                terms = query.split(' ')
                print head
                print sdm_query(terms)
        else:
            "Wrong options!"
