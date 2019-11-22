#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Evan T. Phelps
#
# Distributed under terms of the MIT license.
"""
Basic cross-tabular-data utilities for data exploration.
"""
##############################################################################
import pandas as pd

from epana.scrubdub import isstring


def count_outer_relations(dfs, names, keycol):
    """Counts the number of occurrences of each `keycol` value in each
    dataframe.

    Returns a dataframe with a row for each distinct `keycol` value in the
    union of dataframes `dfs`.  Each column contains the number of `keycol`
    value occurrences in each corresponding dataframe.

    The argument `names` is a list of column names to be assigned to the
    columns of the returned dataframe of counts.  The argument `keycol` may be
    a string that indicates the common column name that is expected to contain
    values that link each dataframe in `dfs`.  It may also be a list of column
    names to support multi-column keys.
    """
    kcol = [keycol] if isstring(keycol) else keycol
    dfs_reduced = [df[kcol].groupby(kcol)[kcol[0]].count()
                   for df in dfs]
    outer_count = pd.concat(dfs_reduced, axis=1, ignore_index=True).fillna(0)
    outer_count.columns = names
    outer_count[keycol] = outer_count.index.values
    return outer_count


def outer_existence_pattern(dfs, names, keycol):
    """Like `count_outer_relations` but returns a record of booleans that
    indicate whether or not each `keycol` value appears in each df of `dfs`
    rather than the number of occurrences of the `keycol` value.
    """
    ex_ptrn = count_outer_relations(dfs, names, keycol)
    ex_ptrn[names] = ex_ptrn[names] > 0
    return ex_ptrn


def count_relational_patterns(dfs, names, keycol):
    """Counts the *count patterns* of the number of relations between the
    dataframes of `dfs`.

    Returns a dataframe of count patterns and number of occurrences.  The sum
    of the number of occurrences is equal to the number of distinct `keycol`
    values in the union of dataframes of `dfs`.

    The argument `names` is a list of column names to be assigned to the
    columns of the returned dataframe of counts.  The argument `keycol` may be
    a string that indicates the common column name that is expected to contain
    values that link each dataframe in `dfs`.  It may also be a list of column
    names to support multi-column keys.
    """
    outer_count = count_outer_relations(dfs, names, keycol)
    # outer_count.reset_index(inplace=True)
    patterns = outer_count.groupby(names).count().reset_index()
    patterns.columns = list(patterns.columns[:-1]) + ['COUNT']
    return patterns.astype(int)


# TODO Maybe refactor to use outer_existence_pattern
def count_existence_patterns(dfs, names, keycol):
    """Like `count_relational_patterns` (see comments) but counts
    *existence patterns* instead of *count patterns*.
    """
    rel_counts = count_relational_patterns(dfs, names, keycol)
    rel_counts[names] = rel_counts[names] > 0
    patterns = rel_counts.groupby(names)['COUNT'].sum().reset_index()
    return patterns
