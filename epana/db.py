#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Evan T. Phelps
#
# Distributed under terms of the MIT license.
"""
Basic DB utilities.
"""
##############################################################################
import datetime

from pandas import Timestamp

import cx_Oracle


class DbOra:

    connstr = '{username}/{password}@{hostname}:{port}/{database}'
#     oracle_connection_string = 'oracle+cx_oracle://' + \
#         '{username}:{password}@{hostname}:{port}/{database}'

    def __init__(self, username, password, hostname, sid, port=1521):
        self._username = username
        self._hostname = hostname
        self._sid = sid
        self._port = port
        self._connection = cx_Oracle.connect(
            DbOra.connstr.format(
                username=username,
                password=password,
                hostname=hostname,
                port=port,
                database=sid
            )
        )
        self._cursor = self._connection.cursor()
        self._cursor.arraysize = 50000

    @property
    def cursor(self):
        return self._cursor

    def put_data(self, name, cnames, data):
        map_dtype_to_oratype = {
            str: 'VARCHAR2(512)',
            type(None): 'VARCHAR2(512)',
            int: 'NUMBER',
            float: 'NUMBER',
            datetime.datetime: 'DATE',
            Timestamp: 'DATE'
        }
        ora_types = [map_dtype_to_oratype[type(x)] for x in data[0]]
        cols_str = ', '.join(['%s %s' % (cname, oratype)
                              for cname, oratype in zip(cnames, ora_types)])
        sql_create = 'create table ' + \
            '%s (%s) ' % (name, cols_str)

        ncols = len(data[0])
        bind_vars_str = ', '.join([':%d' % (i + 1) for i in range(ncols)])
        sql_load = 'insert into %s (%s) values (%s)' % (
            name, ', '.join(list(cnames)), bind_vars_str)
        self._cursor.execute(sql_create)
        self._cursor.prepare(sql_load)
        self._cursor.executemany(None, data)

    def stage_data(self, name, cnames, data):
        map_dtype_to_oratype = {
            str: 'VARCHAR2(512)',
            type(None): 'VARCHAR2(512)',
            int: 'NUMBER',
            float: 'NUMBER',
            datetime.datetime: 'DATE',
            Timestamp: 'DATE'
        }
        ora_types = [map_dtype_to_oratype[type(x)] for x in data[0]]
        cols_str = ', '.join(['%s %s' % (cname, oratype)
                              for cname, oratype in zip(cnames, ora_types)])
        sql_create = 'create global temporary table ' + \
            '%s (%s) on commit preserve rows' % (name, cols_str)

        ncols = len(data[0])
        bind_vars_str = ', '.join([':%d' % (i + 1) for i in range(ncols)])
        sql_load = 'insert into %s (%s) values (%s)' % (
            name, ', '.join(list(cnames)), bind_vars_str)
        try:
            self._cursor.execute(sql_create)
        except cx_Oracle.DatabaseError as dberr:
            error, = dberr.args
            if error.code == 955:
                pass
            else:
                raise
        self._cursor.prepare(sql_load)
        self._cursor.executemany(None, data)

    def execute(self, sql):
        self._cursor.execute(sql)

    def query(self, sql):
        self._cursor.execute(sql)
        for rec in self._cursor:
            yield rec

    def get_column_names(self):
        return [x[0] for x in self._cursor.description]

    def commit(self):
        self._connection.commit()

    def rollback(self):
        self._connection.rollback()

    def __del__(self):
        self._connection.rollback()
        self._connection.close()
