import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
import os
import pandas as pd
from collections import defaultdict

from IPython.display import HTML
from IPython.display import display_html
import sys
sys.path.append("..")
from utils.utils import *

import networkx as nx
from networkx.readwrite import json_graph
import pickle

def load_sql_rep(fn, dummy=None):
    assert ".pkl" in fn
    try:
        with open(fn, "rb") as f:
            query = pickle.load(f)
    except Exception as e:
        print(e)
        print(fn + " failed to load...")
        exit(-1)

    query["subset_graph"] = nx.OrderedDiGraph(json_graph.adjacency_graph(query["subset_graph"]))
    query["join_graph"] = json_graph.adjacency_graph(query["join_graph"])
    if "subset_graph_paths" in query:
        query["subset_graph_paths"] = nx.OrderedDiGraph(json_graph.adjacency_graph(query["subset_graph_paths"]))

    return query

SQL_DIR = "multi_column_1/"
WK = "ceb"
QDIR = "./imdb-new-sqls/" + SQL_DIR
OUTPATH = os.path.join("./imdb-new-dfs/" + SQL_DIR)
make_dir(OUTPATH)
COLS_TO_TABLES = {}

qdict = {}
qfns = []
QREP_DIR = './imdb-new-workload/' + SQL_DIR
if WK in ["job", "ceb"]:
    qfns = os.listdir(QREP_DIR)
    for fn in qfns:
        qfn = os.path.join(QREP_DIR, fn)
        if ".pkl" in qfn:
            qdict[fn] = load_sql_rep(qfn)
            
FNS = os.listdir(QDIR)

sqls = []
qreps = []

for fi, fn in enumerate(FNS):
    if ".sql" not in fn:
        continue
    if "all" in fn:
        continue
    
    fnkey = fn.replace(".sql", ".pkl")
    qreps.append(qdict[fnkey])
        
    fn = os.path.join(QDIR, fn)
    with open(fn, "r", encoding='utf-8') as f:
        sql = f.read()

    sql = sql.replace("IN", "in")
    sql = sql.replace("In", "in")
    sql = sql.replace("ILIKE", "like")
    sql = sql.replace("LIKE", "like")
    
    if "return_rank" in sql:
        print(sql)
    sqls.append(sql)

for i, sql in enumerate(sqls):
    sql = sql.replace("IN", "in")
    sql = sql.replace("In", "in")
    sql = sql.replace("ILIKE", "like")
    sql = sql.replace("LIKE", "like")
    sqls[i] = sql

parse_sql_preds(sql)

start = time.time()
opdf,exprdf = parse_sqls_par(sqls, COLS_TO_TABLES)
print("parse sqls took(s): ", int(time.time()-start))
print("Num Ops: ", len(opdf), "Num Expressions: ", len(exprdf))

print("len(qreps) is ",len(qreps))
if len(qreps) != 0:

    rowcounts = []
    totals = []
    sels = []
    filtersqls = []
    
    for _,row in exprdf.iterrows():
        jid = row["jobid"]
        qrep = qreps[int(jid)]
        sql = sqls[int(jid)]
        try:
            cards = qrep["subset_graph"].nodes()[tuple([row["alias"],])]["cardinality"]
            rowcounts.append(cards["actual"])
            totals.append(cards["total"])
            sels.append(min(float(cards["actual"]) / cards["total"], 1.0))
            
            filtersql = row["filtersql"]
            real_name = qrep["join_graph"].nodes()[row["alias"]]["real_name"]
            filtersql = filtersql.replace(row["input"], real_name, 1)
            filtersqls.append(filtersql)
        except:
            rowcounts.append(-1)
            totals.append(-1)
            sels.append(-1)
            filtersqls.append(-1)
        
if len(qreps) == 0:
    exprdf["InputCardinality"] = -1
    exprdf["RowCount"] = -1
    exprdf["Selectivity"] = -1
else:
    exprdf["InputCardinality"] = totals
    exprdf["Selectivity"] = sels
    exprdf["RowCount"] = rowcounts
    exprdf["filtersql"] = filtersqls
    

edf = exprdf[["exprhash", "InputCardinality", "Selectivity", "RowCount"]]
opdf = opdf.merge(edf, on="exprhash", how="inner")
opdf["workload"] = WK
exprdf["workload"] = WK
opdf["db"] = WK
exprdf["db"] = WK

import time
start = time.time()

opfn = os.path.join(OUTPATH, "op_df.csv")
exprfn = os.path.join(OUTPATH, "expr_df.csv")

opdf.to_csv(opfn, index=False)
print("opdf to csv took: ", time.time()-start)

exprdf.to_csv(exprfn, index=False)