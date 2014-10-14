from __future__ import print_function
import sys
import os
from copy import copy
from table import Table
from elasticsearch import Elasticsearch
from elasticsearch import helpers

es = Elasticsearch([
    {'host': 'localhost'}
])

TABLE = 'table'
ROW = 'row'
COLUMN = 'column'

table_index_template = {
    "_index":TABLE,
    "_type":TABLE
}
row_index_template = {
    "_index":TABLE,
    "_type":ROW
}
col_index_template = {
    "_index":TABLE,
    "_type":COLUMN
}
table_id = 0
row_id = 0
column_id = 0

if es.indices.exists(index=TABLE):
    es.indices.delete(index=TABLE)

es.indices.create(index=TABLE)

path = '../Data/test/'
subfolders = [os.path.join(path, f) for f in os.listdir(path)
              if os.path.isdir(os.path.join(path, f))]

for folder in subfolders:
    doclist = Table.load_from_path(folder)
    table_actions = []
    row_actions = []
    column_actions = []
    for doc, tlist in doclist:
        for t in tlist:
            table_body = {}
            row_body = {}
            headrowlist = [] # for column convinience
            datarowlist = []
            # Header Rows
            headers = {}
            for i in range(len(t.data[0])):
                hdrow = t.data[0][i]
                headrowlist.append(hdrow)
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
                datarowlist.append(dtrow)
                dtag = 'data_row_{0}'.format(i)
                for j in range(len(dtrow)):
                    cell = dtrow[j]
                    ctag = 'value_{0}'.format(j)
                    key = '.'.join([dtag, ctag])
                    row[key] = cell.content
                table_body.update(row)
                row['table_id'] = table_id
                row.update(headers)
                action = copy(row_index_template)
                action["_source"] = row
                action["_id"] = row_id
                row_actions.append(action)
                row_id += 1
            # Data Columns
            rowlist = headrowlist + datarowlist
            if len(rowlist) > 0:
                width = len(rowlist[0]) # Assume the width of rows are the same
                same_width = True
                for i in range(1, len(rowlist)):
                    if len(rowlist[i]) != width:
                        same_width = False
                if same_width:
                    row_headers = {}
                    for i in range(len(headrowlist)):
                        tag = 'row_head.header_{0}'.format(i)
                        row_headers[tag] = headrowlist[i][0].content
                    for i in range(len(datarowlist)):
                        tag = 'row_head.data_{0}'.format(i)
                        row_headers[tag] = datarowlist[i][0].content
                    for i in range(1, width):
                        col = {}
                        col_tag = 'col_{0}'.format(i)
                        for j in range(len(headrowlist)):
                            dtag = 'header_{0}'.format(j)
                            key = '.'.join([col_tag, dtag])
                            col[key] = headrowlist[j][i].content
                        for j in range(len(datarowlist)):
                            dtag = 'value_{0}'.format(j)
                            key = '.'.join([col_tag, dtag])
                            col[key] = datarowlist[j][i].content
                        col['table_id'] = table_id
                        col.update(row_headers)
                        action = copy(col_index_template)
                        action["_source"] = col
                        action["_id"] = column_id
                        column_actions.append(action)
                        column_id += 1
            # Header and Footnote
            if t.caption is not None:
                table_body[Table.CAPTION_TAG] = t.caption
            if t.footnote is not None:
                table_body[Table.FOOTNOTE_TAG] = t.footnote
            action = copy(table_index_template)
            action["_source"] = table_body
            action["_id"] = table_id
            table_actions.append(action)
            table_id += 1

    res = helpers.bulk(es, table_actions, stats_only=True)
    print('{0} tables indexed, {1} failed.'.format(res[0], res[1]), file=sys.stderr)
    res = helpers.bulk(es, row_actions, stats_only=True)
    print('{0} rows indexed, {1} failed.'.format(res[0], res[1]), file=sys.stderr)
    res = helpers.bulk(es, column_actions, stats_only=True)
    print('{0} columns indexed, {1} failed.'.format(res[0], res[1]), file=sys.stderr)
    print()

#es.indices.flush(index='table')
#query={
    #"query": {
        #"query_string" : {
            #"fields" : ["col_*.value_0"],
            #"query" : "lipids"
        #}
    #}
#}
#res = es.search(index='table', doc_type=COLUMN, body=query)
#for row in res['hits']['hits']:
    #print(row)
