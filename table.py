from __future__ import print_function

import sys
import os
import glob
import xml.etree.ElementTree as ET
from utils import *

class Table(object):
    """ Represent the content and layout information of a table in documents. """
    TABLE_TAG = 'table'
    CAPTION_TAG = 'caption'
    FOOTNOTE_TAG = 'footnote'
    HEADERS_TAG = 'headers'
    HEADER_TAG = 'header'
    ROW_TAG = 'row'
    VALUE_TAG = 'value'

    @staticmethod
    def load_from_path(path):
        print("Loading...", file=sys.stderr)
        retlist = []
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
        return retlist

    @staticmethod
    def get_table_list(root):
        retlist = []
        for table in root.iter(Table.TABLE_TAG):
            retlist.append(Table(xmlnode=table))
        return retlist

    @staticmethod
    def get_doc_info(root):
        pass

    def __init__(self, xmlnode=None):
        if xmlnode is not None:
            node = get_single_node(xmlnode, Table.CAPTION_TAG)
            if node is not None:
                self.caption = node.text
            #node = get_single_node(xmlnode, Table.FOOTNOTE_TAG)
            #if node is not None:
                #self.footnote = node.text
            headers_list = get_cell_rows(xmlnode, Table.HEADERS_TAG, Table.HEADER_TAG)
            rows_list = get_cell_rows(xmlnode, Table.ROW_TAG, Table.VALUE_TAG)
            self.data = [headers_list, rows_list]
        else:
            self.caption = None
            self.footnote = None
            self.data = None
