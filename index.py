#! /opt/python27/bin/python
from __future__ import print_function
import sys
import os
from copy import copy
from table import Table
from elasticsearch import Elasticsearch
from elasticsearch import helpers

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

def list2cells(prefix, suffix, targetdict, srclist, col):
    for i in range(len(srclist)):
        key = '{0}.{1}{2}'.format(prefix, suffix, i)
        targetdict[key] = srclist[i][col].content

def get_header_rows(table):
    """ Get header information from a given table.

    return values:
        headers -- headers in dict for json
        headrowlist -- header rows for column construction
    """
    headers = {}
    headrowlist = []
    for i in range(len(table.data[0])):
        hdrow = table.data[0][i]
        headrowlist.append(hdrow)
        htag = 'header_row_{0}'.format(i)
        for j in range(len(hdrow)):
            header = hdrow[j]
            ctag = 'header_{0}'.format(j)
            key = '.'.join([htag, ctag])
            headers[key] = header.content
    return headers, headrowlist

def get_data_rows(table, global_info, headers):
    """ Get data information from a given table.

    arguments:
        global_info -- global information to be associated with each data row
        headers -- header information to be associated with each data row

    return values:
        datarows -- data rows in dict to update table information
        datarowlist -- data rows for column construction
        actionlist -- actions for index the data rows
    """
    global table_id
    global row_id
    actionlist = []
    datarowlist = []
    datarows = {}
    for i in range(len(table.data[1])):
        row = {}
        dtrow = table.data[1][i]
        datarowlist.append(dtrow)
        dtag = 'data_row_{0}'.format(i)
        for j in range(len(dtrow)):
            cell = dtrow[j]
            ctag = 'value_{0}'.format(j)
            key = '.'.join([dtag, ctag])
            row[key] = cell.content
        datarows.update(row)
        row['table_id'] = table_id
        row.update(headers)
        row.update(global_info)
        action = copy(row_index_template)
        action["_source"] = row
        action["_id"] = row_id
        actionlist.append(action)
        row_id += 1
    return datarows, datarowlist, actionlist

def get_columns(table, headrowlist, datarowlist, global_info):
    """ Get columns information from a given table.

    arguments:
        headrowlist: header rows
        datarowlist: data rows
        global_info: global information to be associated with each data column

    return values:
        actionlist -- actions for index the columns
    """
    global table_id
    global column_id
    actionlist = []
    rowlist = headrowlist + datarowlist
    if len(rowlist) > 0:
        width = len(rowlist[0]) # Assume the width of rows are the same
        same_width = True
        for i in range(1, len(rowlist)):
            if len(rowlist[i]) != width:
                same_width = False
        if same_width:
            row_headers = {}
            list2cells('row_head', 'header_', row_headers, headrowlist, 0)
            list2cells('row_head', 'data_', row_headers, datarowlist, 0)
            for i in range(1, width):
                col = {}
                col_tag = 'col_{0}'.format(i)
                list2cells(col_tag, 'header_', col, headrowlist, i)
                list2cells(col_tag, 'value_', col, datarowlist, i)
                col['table_id'] = table_id
                col.update(row_headers)
                col.update(global_info)
                action = copy(col_index_template)
                action["_source"] = col
                action["_id"] = column_id
                actionlist.append(action)
                column_id += 1
    return actionlist

def get_bulk_body(doclist):
    global table_id
    global column_id
    table_actions = []
    row_actions = []
    column_actions = []
    for doc, tlist in doclist:
        for table in tlist:
            global_info = copy(doc)
            # Header and Footnote
            if table.caption is not None:
                global_info[Table.CAPTION_TAG] = table.caption
            if table.footnotes is not None:
                i = 0
                for fn in table.footnotes:
                    global_info['footnote_{0}'.format(i)] = fn
                    i += 1
            # Contexts
            if table.citations is not None:
                i = 0
                for ctx in table.citations:
                    global_info['heading_{0}'.format(i)] = ctx[0]
                    global_info['citation_{0}'.format(i)] = ctx[1]
                    i += 1
            # Header Rows
            headers, headrowlist = get_header_rows(table)
            table_body = copy(headers)
            # Data Rows
            datarows, datarowlist, actionlist = get_data_rows(table, global_info, headers)
            table_body.update(datarows)
            row_actions += actionlist
            # Data Columns
            actionlist = get_columns(table, headrowlist, datarowlist, global_info)
            column_actions += actionlist
            # Table
            table_body.update(global_info)
            action = copy(table_index_template)
            action["_source"] = table_body
            action["_id"] = table_id
            table_actions.append(action)
            table_id += 1
    return table_actions, row_actions, column_actions

def bulk_index(service, actions, unit_name):
    res = helpers.bulk(service, actions, stats_only=True)
    print('{0} {1} indexed, {2} failed.'.format(res[0], unit_name, res[1]), file=sys.stderr)


es = Elasticsearch([
    {'host': 'compute-1-32'}
])

if es.indices.exists(index=TABLE):
    es.indices.delete(index=TABLE)
es.indices.create(index=TABLE, body='{"settings":{"index":{"analysis":{"analyzer":{"default":{"type":"english"}}}}}}')
#es.indices.close(index=TABLE)
#es.indices.put_settings(body='{"analysis":{"analyzer":{"default":{"type":"english"}}}}', index=TABLE)
#es.indices.open(index=TABLE)

path = '../data/'
subfolders = [os.path.join(path, f) for f in os.listdir(path)
              if os.path.isdir(os.path.join(path, f))]

for folder in subfolders:
    doclist = Table.load_from_path(folder)
    table_actions, row_actions, column_actions = get_bulk_body(doclist)

    bulk_index(es, table_actions, 'tables')
    bulk_index(es, row_actions, 'rows')
    bulk_index(es, column_actions, 'columns')
    print(file=sys.stderr)
