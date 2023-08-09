import copy
import numpy as np
import pandas as pd

from ortools.sat.python import cp_model
import collections

from loki.util import printer


def build_model(program, constraints, leftover_constraints, table_cardinality, vars_per_col=1000):
    cols = list(set([c[0] for cs in constraints.keys() for c in cs]))
    col_values = {col: list(set([c[2] for cs in constraints.keys() for c in cs if c[0] == col])) for col in cols}
    values = [v for col in cols for v in col_values[col]]
    col_values_map = {col: {v: k + 1 for k, v in enumerate(col_values[col])} for col in cols}
    col_values_ids_map = {col: {v: k for k, v in col_values_map[col].items()} for col in cols}

    model = cp_model.CpModel()
    vars = collections.defaultdict(list)

    # Create row variables
    scale = 0
    for i, col in enumerate(cols):
        for var in range(i * vars_per_col, (i + 1) * vars_per_col):
            vars[var].append(model.NewBoolVar('%s_%i_%s' % (col, var, 'NULL')))
            for value in col_values[col]:
                vars[var].append(model.NewBoolVar('%s_%i_%s' % (col, var, value)))
                scale += 1
    print(f'Problem scale: {scale}')

    # Enforce exactly one value per variable
    for var in range(len(vars)):
        model.Add(sum(vars[var]) == 1)

    constraint_id = 0
    total_rows = 0
    i = 0
    has_like = False
    for k, v in constraints.items():
        downsampled_v = int(v / table_cardinality * vars_per_col)
        total_rows += downsampled_v
        i += 1
        # XXX: ideally figure out a better input format where these things are not a concern here
        has_like = has_like or any([ks[1] == 'like' for ks in k])
        if has_like and total_rows > vars_per_col:  # Ensure there is always a solution
            print(f'Stopping after {i} constraints!')
            break
        if downsampled_v == 0:  # Collect constraints that are lost due to downsampling
            leftover_constraints.append(k)
            continue
        else:
            pass  # Move forward with constraint encoding
        if len(k) == 1:  # No correlation
            k = k[0]
            model.Add(sum(vars[cols.index(k[0]) * vars_per_col + var][col_values_map[k[0]][k[2]]] for var in range(vars_per_col)) == downsampled_v)
        else:  # Correlated columns: we must ensure that the values match on the same rows
            correlated_columns = [c[0] for c in k]
            correlated_values = [c[2] for c in k]
            correlations = list(zip(correlated_columns, correlated_values))
            vs = [model.NewIntVar(0, 1, 'tmp_%i_%i' % (constraint_id, i)) for i in range(vars_per_col)]  # range of values for var is (0, 1) because it's either 1*1 or 0*something
            for i in range(vars_per_col):
                model.AddMultiplicationEquality(vs[i], [vars[cols.index(ccol) * vars_per_col + i][col_values_map[ccol][cvalue]] for ccol, cvalue in correlations])
            model.Add(sum(vs) == downsampled_v)
        constraint_id += 1

    return model, vars, cols, col_values_ids_map


def get_solution(solver, vars):
    solution = []
    for var in range(len(vars)):
        for value in range(len(vars[var])):
            if solver.BooleanValue(vars[var][value]):
                solution.append(value)
    return solution


def solve(model, vars, cols, col_values_ids_map, vars_per_col=1000):
    solver = cp_model.CpSolver()
    status = solver.Solve(model)

    print('Status = %s' % solver.StatusName(status))
    abstract_solution = get_solution(solver, vars)

    solution = [col_values_ids_map[cols[int(i / vars_per_col)]][v] if v in col_values_ids_map[cols[int(i / vars_per_col)]] else None for i, v in enumerate(abstract_solution)]
    return {cols[int(i / vars_per_col)]: solution[i:i + vars_per_col] for i in range(0, len(solution), vars_per_col)}
