import json
from elasticsearch import Elasticsearch

def search_table(es, query_terms, search_type="phrase"):
    query = {
        "query": {
            "multi_match": {
                "query" : " ".join(query_terms),
                "type": search_type,
                "fields" : ["caption", "footnote_*", "heading_*", "citation_*", "header_rows.header_*", "data_rows.svalue_0"]
            }
        },
        "highlight" : {
            "fields": {
                "header_rows.header_*" : {},
                "data_rows.svalue_0" : {},
                "caption" : {},
                "footnote_*" : {},
                "heading_*" : {},
                "citation_*" : {}
            }
        }
    }
    res = es.search(index='table', doc_type='table', body=query, size=1)
    res['hits']['hits'].reverse()
    #print json.dumps(res['hits']['hits'], sort_keys=True, indent=4)
    count = res['hits']['total']
    #print "{0} results found.".format(count)
    return count

def search_row(es, query_terms):
    query = {
        "query": {
            "multi_match": {
                "query" : " ".join(query_terms),
                "type": "cross_fields",
                "fields" : ["header_row*", "data_row_*.value_0"]
            }
        }
    }
    res = es.search(index='table', doc_type='row', body=query)
    res['hits']['hits'].reverse()
    print json.dumps(res['hits']['hits'], sort_keys=True, indent=4)
    print "{0} results returned.".format(len(res['hits']['hits']))

def search_column(es, query_terms):
    query = {
        "query": {
            "multi_match": {
                "query" : " ".join(query_terms),
                "type": "cross_fields",
                "fields" : ["row_header_*", "col_*.header_*"]
            }
        }
    }
    res = es.search(index='table', doc_type='column', body=query)
    res['hits']['hits'].reverse()
    print json.dumps(res['hits']['hits'], sort_keys=True, indent=4)
    print "{0} results returned.".format(len(res['hits']['hits']))

if __name__ == '__main__':
    es = Elasticsearch([
        {'host': 'compute-2-15', 'port': 14010}
    ])

    while True:
        line = raw_input()
        argv= line.split(' ')
        doc_type = argv[0]
        query_terms = argv[1:]
        if doc_type.lower() == 'table':
            search_table(es, query_terms)
        elif doc_type.lower() == 'row':
            search_row(es, query_terms)
        elif doc_type.lower() == 'column':
            search_column(es, query_terms)
        elif doc_type.lower() == 'exit':
            exit()
        else:
            print "Type can only be: table, row, column"
