#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Evan T. Phelps
#
# Distributed under terms of the MIT license.
"""
Basic tabular-data utilities for preliminary data charactization.
"""
##############################################################################
import csv
import os

import ftfy

import numpy as np

import pandas as pd

from epana.logutils import get_logger
from epana.logutils import mstime

from epana.scrubdub import guess_encoding
from epana.scrubdub import isstring
from epana.scrubdub import iterable_to_stream


def guess_dialect(fn):
    """Return CSV dialect assumed by csv package"""
    dialect = None
    # , encoding=guess_encoding(fn), newline='') as fin:
    with open(fn, 'r') as fin:
        dialect = csv.Sniffer().sniff(fin.read(4096 * 10))
    return dialect


def get_df_raw(fn, fix_unicode=False):
    """Return a dataframe of character string types.

    Useful if you don't want to let Pandas automatically determine the data
    types, for example, in first steps of input data evaluation.

    WARNING: using fix_unicode=True is very slow!  Might be better to fix and
    copy (see fix_unicode_and_copy) the file for future use, if required.
    In my experieence, it is relatively rare to have to do this anyway.
    """
    LOGNAME = '%s:%s' % (os.path.basename(__file__), 'get_df_raw()')
    log = get_logger(LOGNAME)
    guess = guess_encoding(fn)
    with open(fn, 'rb') as fin:
        if fix_unicode:
            log.warn('! Fixing unicode: This may take some time!')
            log.info('creating unicode generator')
            fin_fixed = ftfy.fix_file(fin, encoding=guess)
            log.info('done creating unicode generator')
            with iterable_to_stream(fin_fixed) as bffr:
                t0 = mstime()
                df = pd.read_csv(bffr, encoding='utf8', dtype=str)
                t1 = mstime()
                log.info('created dataframe from fixed unicode of ' +
                         '%s: %d x %d (%d msecs)' % (fn,
                                                     len(df),
                                                     len(df.columns),
                                                     t1 - t0))
                return df
        else:
            t0 = mstime()
            df = pd.read_csv(fin, encoding=guess, dtype=str)
            t1 = mstime()
            log.info('created dataframe ' +
                     '%s: %d x %d (%d msecs)' % (fn,
                                                 len(df),
                                                 len(df.columns),
                                                 t1 - t0))
            return df


def print_full(x):
    with pd.option_context(
            'display.max_rows', len(x),
            'display.max_columns', len(x.columns) + 1,
            'display.width', 1000,
            'display.max_colwidth', 64):
        print(x)

def n_null(s):
    return (s.isnull() | (s.astype(str).str.rstrip().str.len() == 0)).sum()


def n_not_null(s):
    return len(s) - n_null(s)


def n_zero(s):
    return sum(s == 0)


def n_most_common(s):
    return int(s.value_counts().max())


def most_common(s):
    return s.value_counts().idxmax()


def n_distinct(s):
    return len(s.value_counts())


def vlen(s):
    s1 = s.apply(str)
    lens = s1.str.len()
    return (np.mean(lens), len(lens.value_counts()), min(lens), max(lens))

def min(s):
    return s.dropna().min()

def max(s):
    return s.dropna().max()

def get_summary(data, navals=None):
        LOGNAME = '%s:%s' % (os.path.basename(__file__), 'get_summary()')
        log = get_logger(LOGNAME)
        df = data
        if not isinstance(df, pd.DataFrame):
            fn_bn = os.path.basename(data)
            log.debug('reading %s' % fn_bn)

            t0 = mstime()
            df = pd.read_csv(data, na_values=navals, low_memory=False)

            log.debug('done reading %s: %d x %d (%d msecs)' % (fn_bn,
                                                               len(df),
                                                               len(df.columns),
                                                               mstime() - t0))

        aggs = [n_not_null, n_null, n_zero, n_distinct, vlen,
                min, max, most_common, n_most_common]
        cols = list([func.__name__ for func in aggs])

        log.debug('generating summary')
        t0 = mstime()
        df_summ = df.apply(aggs).T
        df_summ = df_summ[[c for c in cols if c in df_summ.columns]]
        df_summ['dtype'] = df.dtypes
        log.debug('done generating summary: (%d msecs)' %
                  (mstime() - t0))

        return df_summ
    # df = data
    # if not isinstance(df, pd.DataFrame):
    #     fn_bn = os.path.basename(data)
    #     df = pd.read_csv(data, na_values=navals, low_memory=False)
    #
    # aggs = [n_not_null, n_null, n_zero, n_distinct, vlen,
    #         min, max, n_most_common, most_common]
    # cols = list([func.__name__ for func in aggs])
    # df_summ = df.apply(aggs).T
    # df_summ = df_summ[[c for c in cols if c in df_summ.columns]]
    # df_summ['dtype'] = df.dtypes
    #
    # return df_summ

# def n_null(s):
#     return (s.isnull() | (s.astype(str).str.rstrip().str.len() == 0)).sum()
#
#
# def n_not_null(s):
#     return len(s) - n_null(s)
#
#
# def n_zero(s):
#     return sum(s == 0)
#
#
# def n_most_common(s):
#     return int(s.value_counts().max())
#
#
# def most_common(s):
#     return s.value_counts().idxmax()
#
#
# def n_distinct(s):
#     return len(s.value_counts())
#
#
# def vlen(s):
#     s1 = s.apply(str)
#     lens = s1.str.len()
#     return (np.mean(lens), len(lens.value_counts()), min(lens), max(lens))
#
#
# def get_summary(data, navals=None):
#     LOGNAME = '%s:%s' % (os.path.basename(__file__), 'get_summary()')
#     log = get_logger(LOGNAME)
#     df = data
#     if not isinstance(df, pd.DataFrame):
#         fn_bn = os.path.basename(data)
#         log.debug('reading %s' % fn_bn)
#
#         t0 = mstime()
#         df = pd.read_csv(data, na_values=navals, low_memory=False)
#
#         log.debug('done reading %s: %d x %d (%d msecs)' % (fn_bn,
#                                                            len(df),
#                                                            len(df.columns),
#                                                            mstime() - t0))
#
#     aggs = [n_not_null, n_null, n_zero, n_distinct, vlen,
#             min, max, most_common, n_most_common]
#     cols = list([func.__name__ for func in aggs])
#
#     log.debug('generating summary')
#     t0 = mstime()
#     df_summ = df.apply(aggs).T
#     df_summ = df_summ[[c for c in cols if c in df_summ.columns]]
#     df_summ['dtype'] = df.dtypes
#     log.debug('done generating summary: (%d msecs)' %
#               (mstime() - t0))
#
#     return df_summ


def load_files(fnames, pwd=None, delims=None, dtype=str,
               quotechar="'", escapechar="'", quoting=csv.QUOTE_NONE,
               usecols=None, error_bad_lines=True):
    df = None
    delims = len(fnames) * ['|'] if delims is None else delims
    for (fname, delim) in zip(fnames, delims):
        with open(fname) as fin:
            ufin = fin
            this_df = pd.read_table(ufin, sep=delim, dtype=dtype,
                                    quotechar=quotechar, quoting=quoting,
                                    usecols=usecols, encoding='utf-8',
                                    error_bad_lines=error_bad_lines)
            this_df['fname'] = fname
            this_df.columns = [c.replace("'", "") for c in this_df.columns]
            df = this_df if df is None else df.append(
                this_df, ignore_index=True)
    return df


def df_from_sql(sql, engine):
    df = pd.read_sql(sql, con=engine)
    return df


def coalesce(df, cols):
    coalesced = df[cols[0]]
    for cname in cols[1:]:
        coalesced = coalesced.combine_first(df[cname])
    return coalesced


def freq(df, attgrp, agglvl=0, multi_idx=False, cumsum=False):
    attsumm = None
    attgrp = [attgrp] if isstring(attgrp) else attgrp
    if len(attgrp) == 1:
        agglvl = 0
        att = attgrp[0]
        attsumm = pd.DataFrame({'COUNT': df[att].value_counts(dropna=False)})
        attsumm.index.names = [att]
    else:
        attsumm = df[attgrp].groupby(attgrp, dropna=False).agg(lambda x: len(x))
        attsumm = attsumm.reset_index(name='COUNT')
    attsumm = attsumm.sort_values(['COUNT'], ascending=[False])
    if agglvl > 0:
        attsumm = attsumm.sort_values(
            attgrp[0:agglvl] + ['COUNT'],
            ascending=['True'] * agglvl + [False])
        attsumm['PERC'] = attsumm.groupby(
            attgrp[0:agglvl], dropna=False).COUNT.apply(lambda x: 100 * x / sum(x))
        if cumsum:
            attsumm['CUMPERC'] = attsumm.groupby(attgrp[0:agglvl], dropna=False).PERC.cumsum()
    else:
        attsumm['PERC'] = 100 * attsumm.COUNT / sum(attsumm.COUNT)
        if cumsum:
            attsumm['CUMPERC'] = attsumm.PERC.cumsum()
    if multi_idx:
        attsumm.set_index(attgrp, inplace=True)
    return attsumm


def get_mem_usage(pandas_obj):
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:  # we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2  # convert bytes to megabytes
    return usage_mb


def shrink_df(df):
    """Determine the smallest sized dtype for each column of the DataFrame
    and replace each column with a reduced copy.
    Reference https://www.dataquest.io/blog/pandas-big-data/.
    """
    df_int = df.select_dtypes(include=['int'])
    df[df_int.columns] = df_int.apply(pd.to_numeric, downcast='unsigned')

    df_float = df.select_dtypes(include=['float'])
    df[df_float.columns] = df_float.apply(pd.to_numeric, downcast='float')

    df_obj = df.select_dtypes(include=['object'])
    for col in df_obj.columns:
        num_unique_values = len(df_obj[col].unique())
        num_total_values = len(df_obj[col])
        if num_unique_values / num_total_values < 0.5:
            df.loc[:, col] = df_obj[col].astype('category')
        else:
            df.loc[:, col] = df_obj[col]


def get_reduced_dtypes(df):
    """Determine and return the smallest sized dtype for each column of the
    DataFrame.  This would normally be used when df is a large sample of an
    even larger dataset yet to be loaded.  The reduced dtypes would then be
    passed to the Pandas read_csv function when the full dataset is being
    read.
    """
    df2 = df.copy()
    shrink_df(df2)
    dtypes = df2.dtypes
    dtypes_col = dtypes.index
    dtypes_type = [i.name for i in dtypes.values]
    column_types = dict(zip(dtypes_col, dtypes_type))
    return column_types


# TODO: move this to bin script
def gen_code_freqs(df_in, cols, fnout):
    xlwrtr = pd.ExcelWriter(fnout, engine='xlsxwriter')

    for col in cols:
        df = freq(df_in, col)
        tabname = col if isstring(col) else '-'.join(col)[0:31]
        df.to_excel(xlwrtr, sheet_name=tabname, index=True)
    xlwrtr.save()
