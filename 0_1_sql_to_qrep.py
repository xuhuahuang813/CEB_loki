from query_representation.query import *
from query_representation.utils import *
from networkx.readwrite import json_graph
import os
import pdb

import sys
sys.path.append("./")


SQL_DIR = "multi_column_3/"  # "./imdb-sqls/"中存在文件夹
INPUT_SQLS_PATH_LIST = os.listdir("./imdb-new-sqls/" + SQL_DIR)
OUTPUT_DIR = "./imdb-new-workload/" + SQL_DIR

make_dir(OUTPUT_DIR)

for i, sql_path in enumerate(INPUT_SQLS_PATH_LIST):
    print(i, end="\r")
    with open("./imdb-new-sqls/" + SQL_DIR + "/" + sql_path, "r", encoding='utf-8') as f:
        sql = f.read()

    output_fn = OUTPUT_DIR + sql_path.replace(".sql", "") + ".pkl"

    if "SELECT" not in sql:
        print("SELECT not in sql")
        continue

    qrep = parse_sql(sql, None, None, None, None, None,
                     compute_ground_truth=False)

    qrep["subset_graph"] = \
        nx.OrderedDiGraph(json_graph.adjacency_graph(qrep["subset_graph"]))
    qrep["join_graph"] = json_graph.adjacency_graph(qrep["join_graph"])

    save_qrep(output_fn, qrep)
