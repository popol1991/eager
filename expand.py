import json
import math
import operator
import re
from table import Table

TERM_NUM = 5
global_info = ['caption']
type_fields = {
        Table.TABLE : [r'header_row', r'data_row_.*value_0'] + global_info,
        Table.ROW : [r'header_row', r'data_row_.*value_0'] + global_info,
        Table.COLUMN : [r'row_head', r'col_.*header.*'] + global_info
        }

def expand(data, content):
    term_vec, score_vec = get_term_vectors(content)
    expand_terms = get_expand_terms(term_vec, score_vec)
    data = json.loads(data)
    equery = expand_query(data, expand_terms).encode('utf-8')
    return json.dumps(make_body(data, equery))

def get_term_vectors(content):
    """ Given the JSON response from ElasticSearch, return document term frequency vector. """
    hits = json.loads(content)['hits']
    resultNum = int(hits['total'])
    if resultNum == 0:
        return None
    doc_type = hits['hits'][0]['_type']
    term_vec = get_type_term_vector(doc_type, hits['hits'])
    score_vec = [float(doc['_score']) for doc in hits['hits']]
    return term_vec, score_vec

def get_expand_terms(term_vec, score_vec):
    """
    score(t) = SUM{ p(t|d)*p(I|d)*log(1/p(t|C)) }
                    tf     ds     idf
    """
    doc_num = len(term_vec)
    vocab = get_vocab(term_vec)
    idf_vec = get_idf_vec(vocab, term_vec)
    score_dict = {}
    for k in range(len(vocab)):
        t = vocab[k]
        score = 0
        for i in range(doc_num):
            doc = term_vec[i]
            if t in doc:
                tf = doc[t]
                ds = score_vec[i]
                idf = idf_vec[k]
                score += tf * ds * math.log(1/idf)
        score_dict[t] = score
    sorted_terms = sorted(score_dict.items(), key=operator.itemgetter(1), reverse=True)
    selected = [t[0] for t in sorted_terms if (t[0].isalnum() and len(t[0])>1)]
    return [w for w in selected[:5]]

def expand_query(data, expand_terms):
    query = data['query']['multi_match']['query']
    return " ".join([query] + expand_terms)

def make_body(data, equery):
    data['query']['multi_match']['query'] = equery
    return data

def get_type_term_vector(doc_type, doclist):
    term_vec = []
    for d in doclist:
        term_vec.append(group_fields(type_fields[doc_type], d['_source']))
    return term_vec

def group_fields(fields, doc):
    termlist = []
    for f in doc:
        for p in fields:
            if re.match(p, f) is not None and doc[f] is not None:
                termlist += re.split('\s+', doc[f])
                break
    term_vec = {}
    for t in termlist:
        if not t in term_vec:
            term_vec[t] = 0
        term_vec[t] += 1
    return term_vec

def get_vocab(term_vec):
    vocab = set()
    for doc in term_vec:
        for t in doc:
            vocab.add(t)
    return list(vocab)

def get_idf_vec(vocab, term_vec):
    idf_vec = []
    C = len(term_vec)
    for t in vocab:
        count = 0
        for d in term_vec:
            if t in d:
                count += 1
        idf_vec.append(count*1.0/C)
    return idf_vec
