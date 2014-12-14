import sys
from gen_query import sdm_query
from gen_query import match_query
from mlt import mlt_query
from elasticsearch import Elasticsearch

TABLE = "table"
INDEX_LIST = ["table", "table_okapi"]
TOPK = 50
TOPK_SIM = 10
ALL_FIELDS = ["caption", "footnote_*", "heading_*", "citation_*", "header_rows.header_*", "data_rows.svalue_0"]

if __name__ == '__main__':
    es = Elasticsearch([
        {"host":"compute-2-15", "port":14010}
    ])

    with open(sys.argv[1]) as fin:
        for line in fin:
            qid, query = line.strip().split(':')
            terms = query.split(' ')
            pool = set()
            for index in INDEX_LIST:
                print >> sys.stderr, "Ranking Model: " + index
                sdmquery = sdm_query(terms, size=TOPK)
                res = es.search(index=index, doc_type=TABLE, body=sdmquery)
                hits = res["hits"]["hits"]
                ids = [hit["_id"] for hit in hits]
                print >> sys.stderr, "{0}/{1} results by SDM.".format(len(set(ids)-(set(ids)&pool)), len(ids))
                pool = pool | set(ids)

                mquery = match_query(terms, size=TOPK)
                res = es.search(index=index, doc_type=TABLE, body=mquery)
                hits = res["hits"]["hits"]
                ids = [hit["_id"] for hit in hits]
                print >> sys.stderr, "{0}/{1} results by BOW.".format(len(set(ids)-(set(ids)&pool)), len(ids))
                pool = pool | set(ids)

                # Expand
                mltquery = mlt_query(ids, size=TOPK, percent = 0.1)
                mltres = es.search(index=index, doc_type=TABLE, body=mltquery)
                mltids = [hit["_id"] for hit in mltres["hits"]["hits"]]
                print >> sys.stderr, "{0}/{1} results by expanded similar tables.".format(len(set(mltids)-(set(mltids)&pool)), len(mltids))
                pool = pool | set(mltids)

                # Single
                sim_pool = set()
                for i in range(min(TOPK_SIM, len(hits))):
                    hittable = hits[i]
                    ids = [hittable["_id"]]
                    mltquery = mlt_query(ids, size=TOPK)
                    mltres = es.search(index=index, doc_type=TABLE, body=mltquery)
                    mltids = [hit["_id"] for hit in mltres["hits"]["hits"]]
                    sim_pool = sim_pool | set(mltids)
                    # For matched fields
                    fields = hittable["highlight"].keys()
                    mltquery = mlt_query(ids, fields=fields, size=TOPK)
                    mltres = es.search(index=index, doc_type=TABLE, body=mltquery)
                    mltids = [hit["_id"] for hit in mltres["hits"]["hits"]]
                    sim_pool = sim_pool | set(mltids)
                print >> sys.stderr, "{0}/{1} results by grouped single similar tables.".format(len(sim_pool-(sim_pool&pool)), len(sim_pool))
                pool = pool | sim_pool
            print query + ": " + str(len(pool))

