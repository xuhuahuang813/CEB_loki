import copy
import numpy as np
import pandas as pd


def apply_leftover_constraints(full_solution, leftover_constraints):
    for lc in leftover_constraints:
        if len(lc) == 1:
            new_vs = full_solution[lc[0][0]]
            for i, v in enumerate(full_solution[lc[0][0]]):
                if v is None:
                    new_vs = new_vs[:i] + [lc[0][2]] + new_vs[i + 1:]
                    break
            full_solution[lc[0][0]] = new_vs


def solution_to_df(full_solution):
    solution_df = pd.DataFrame(full_solution)
    solution_df = solution_df.applymap(lambda x: np.nan if x is None else x)
    return solution_df


def scale_solution_df(solution_df, table_cardinality, vars_per_col):
    real_row_size = int(table_cardinality / vars_per_col)
    newdf = pd.DataFrame(np.repeat(solution_df.values, real_row_size, axis=0))
    newdf.columns = solution_df.columns
    final_df = newdf.reindex(list(range(0, table_cardinality))).reset_index(drop=True)
    return final_df
