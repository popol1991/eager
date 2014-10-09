from __future__ import print_function
from copy import copy
from table import Table
from elasticsearch import Elasticsearch

es = Elasticsearch([
    {'host': 'localhost'}
])

TABLE = 'table'
ROW = 'row'

if es.indices.exists(index=TABLE):
    es.indices.delete(index=TABLE)

es.indices.create(index=TABLE)

doclist = Table.load_from_path('../Data/test/')
for doc, tlist in doclist:
    for t in tlist:
        table_body = {}
        row_body = {}
        # Header Rows
        headers = {}
        for i in range(len(t.data[0])):
            hdrow = t.data[0][i]
            htag = 'header_row_{0}'.format(i)
            for j in range(len(hdrow)):
                header = hdrow[j]
                ctag = 'header_{0}'.format(j)
                key = '.'.join([htag, ctag])
                headers[key] = header.content
        table_body = copy(headers)
        # Data Rows
        for i in range(len(t.data[1])):
            row = {}
            dtrow = t.data[1][i]
            dtag = 'data_row_{0}'.format(i)
            for j in range(len(dtrow)):
                cell = dtrow[j]
                ctag = 'value_{0}'.format(j)
                key = '.'.join([dtag, ctag])
                row[key] = cell.content
            table_body.update(row)
            row.update(headers)
            es.index(index=TABLE, doc_type=ROW, body=row)
        # Header and Footnote
        if t.caption is not None:
            table_body[Table.CAPTION_TAG] = t.caption
        res = es.index(index=TABLE, doc_type=TABLE, body=table_body)

#res = es.indices.status(index='table')
#print(res)
es.indices.flush(index='table')
query={
    "query": {
        "query_string" : {
            "fields" : ["data_row_*.value_1"],
            "query" : "Diet"
        }
    }
}
res = es.search(index='table', doc_type=ROW, body=query)
for row in res['hits']['hits']:
    print(row)
