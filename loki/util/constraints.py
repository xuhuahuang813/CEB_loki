import copy
import numpy as np
import pandas as pd


def get_constraints_df(c_df, table, max_cols=2):
    constraints_df = c_df.where(c_df['input'] == table).dropna()
    constraints_df['opstring'] = constraints_df['Column'] + constraints_df['Op'] + constraints_df['Value']
    constraints_df = constraints_df.join(constraints_df['Column'].str.split('|', expand=True).add_prefix('Column'))
    constraints_df = constraints_df.join(constraints_df['Op'].str.split('|', expand=True).add_prefix('Op'))
    constraints_df = constraints_df.join(constraints_df['Value'].str.split('|', expand=True).add_prefix('Value'))
    for col in [f'Column{i}' for i in range(max_cols)]:
        constraints_df[col] = constraints_df[col].str.extract(r'\S+\.(\S+)')
    return constraints_df


def get_table_cardinality(constraints_df):
    return int(constraints_df['InputCardinality'].max())


def get_co_optimized_columns(constraints_df, columns, max_cols=2):
    co_optimized_cols = {column: set() for column in columns}
    for row in constraints_df[constraints_df.columns & [f'Column{i}' for i in range(max_cols)]].iterrows():
        combo = [x for x in list(row[1].dropna()) if x is not None]
        if len(combo) > 1:
            for col in combo:
                cols = set(combo)
                cols.remove(col)
                co_optimized_cols[col] |= cols
    return co_optimized_cols


def get_programs(co_optimized_cols):
    programs = []
    for col, co in co_optimized_cols.items():
        closure = set([col]) | co
        prev_len = 0
        while len(closure) > prev_len:
            prev_len = len(closure)
            new_closure = copy.deepcopy(closure)
            for col2 in closure:
                new_closure |= co_optimized_cols[col2]
            closure = new_closure
        if closure not in programs:
            programs.append(closure)
    return programs


def parse_constraints(program, constraints_df):
    constraints = {}
    for row in constraints_df.itertuples():  # Deal with input format mess
        columns = [row.Column0, row.Column1] if not pd.isnull(row.Column0) and not pd.isnull(row.Column1) else [row.Column0]
        # columns = [row.Column0]
        if len(set(columns).intersection(set(program))) == 0:  # Kick out constraints that are not for this program
            continue
        ops = [row.Op0, row.Op1] if not pd.isnull(row.Op0) and not pd.isnull(row.Op1) else [row.Op0]
        # ops = [row.Op0]
        values = [row.Value0, row.Value1] if not pd.isnull(row.Value0) and not pd.isnull(row.Value1) else [row.Value0]
        # values = [row.Value0]
        
        values = [value.replace('\'', '') for value in values]
        # Sorting by col ensures proper uniqueness above
        row_constraints = tuple(sorted([(col, op, val) for col, op, val in zip(columns, ops, values)]))
        cardinality = int(row.RowCount)
        if row_constraints not in constraints or constraints[row_constraints] < cardinality:
            constraints[row_constraints] = cardinality
    return constraints
