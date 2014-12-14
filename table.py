from __future__ import print_function

import sys
import os
import glob
import xml.etree.ElementTree as ET
from utils import *

class Table(object):
    TABLE = 'table'
    ROW = 'row'
    COLUMN = 'column'
    """ Represent the content and layout information of a table in documents. """
    TABLE_TAG = 'table'
    CAPTION_TAG = 'caption'
    FOOTNOTE_TAG = 'footnote'
    HEADERS_TAG = 'headers'
    HEADER_TAG = 'header'
    CONTEXT_TAG = 'context'
    SENTENCE_TAG = 'sentence'
    ROW_TAG = 'row'
    VALUE_TAG = 'value'
    CE = '{http://www.elsevier.com/xml/common/dtd}'

    @staticmethod
    def load_from_path(path):
        print("Loading from {0}...".format(os.path.abspath(path)), file=sys.stderr)
        retlist = []
        curdir = os.path.abspath(os.curdir)
        os.chdir(path)
        files = glob.glob('*.xml')
        count = 0
        for f in files:
            count += 1
            print("{0}\r".format(count), file=sys.stderr, end='')
            sys.stderr.flush()
            root = ET.parse(f).getroot()
            doc = Table.get_doc_info(root)
            tablelist = Table.get_table_list(root)
            retlist.append(( doc, tablelist ))
        os.chdir(curdir)
        return retlist

    @staticmethod
    def get_table_list(root):
        retlist = []
        for table in root.iter(Table.TABLE_TAG):
            retlist.append(Table(xmlnode=table))
        return retlist

    @staticmethod
    def get_doc_info(root):
        doc = {}
        meta = root.find('metadata')
        for child in meta:
            doc[child.tag] = child.text
        title = root.find('article-title')
        doc['article-title'] = title.text
        doc.update(items_from_xml_list(root, 'authors', 'author'))
        doc.update(items_from_xml_list(root, 'keywords', 'keyword'))
        return doc

    def __init__(self, xmlnode=None):
        if xmlnode is not None:
            # caption
            node = get_single_node(xmlnode, Table.CAPTION_TAG)
            if node is not None:
                self.caption = node.text
            else:
                self.caption = None
            self.footnote = None
            # footnote
            self.footnotes = []
            for fn in xmlnode.iter(Table.FOOTNOTE_TAG):
                self.footnotes.append(fn.text)
            # context
            self.citations = []
            for ctx in xmlnode.iter(Table.CONTEXT_TAG):
                heading_node = ctx.find('headings/{0}section-title'.format(Table.CE))
                sentence_node = ctx.find('citation/sentence')
                heading = heading_node.text if heading_node is not None else None
                sentence = sentence_node.text if sentence_node is not None else None
                self.citations.append((heading, sentence))
            # rows
            headers_list = get_cell_rows(xmlnode, Table.HEADERS_TAG, Table.HEADER_TAG)
            rows_list = get_cell_rows(xmlnode, Table.ROW_TAG, Table.VALUE_TAG)
            self.data = [headers_list, rows_list]
        else:
            self.caption = None
            self.footnotes = None
            self.data = None
            self.citations = None
