# sqlalchemy_teradata/dialect.py
# Copyright (C) 2015-2016 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy.engine import default
from sqlalchemy import pool
from sqlalchemy.sql import select, and_, or_
from sqlalchemy_teradata.compiler import TeradataCompiler, TeradataDDLCompiler,\
                                         TeradataTypeCompiler
from sqlalchemy_teradata.base import TeradataIdentifierPreparer, TeradataExecutionContext
from sqlalchemy.sql.expression import text, table, column
from sqlalchemy import Table, Column, Index

class TeradataDialect(default.DefaultDialect):

    name = 'teradata'
    driver = 'teradata'
    default_paramstyle = 'qmark'
    poolclass = pool.SingletonThreadPool

    statement_compiler = TeradataCompiler
    ddl_compiler = TeradataDDLCompiler
    type_compiler = TeradataTypeCompiler
    preparer = TeradataIdentifierPreparer
    execution_ctx_cls = TeradataExecutionContext

    supports_native_boolean = False
    supports_native_decimal = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    postfetch_lastrowid = False
    implicit_returning = False
    preexecute_autoincrement_sequences = False

    construct_arguments = [
      (Table, {
              "post_create": None,
              "postfixes": None
       }),

      (Index, {
          "order_by": None,
          "loading": None
       }),

      (Column, {
          "compress": None,
          "identity": None
      })
    ]

    def __init__(self, **kwargs):
        super(TeradataDialect, self).__init__(**kwargs)

    def create_connect_args(self, url):
      if url is not None:
        params = super(TeradataDialect, self).create_connect_args(url)[1]
        return (
                ("Teradata",
                        params['host'],
                        params['username'],
                        params['password']),
                {})

    @classmethod
    def dbapi(cls):
        """ Hook to the dbapi2.0 implementation's module"""
        from teradata import tdodbc
        return tdodbc

    def normalize_name(self, name, **kw):
        return name.lower()

    def has_table(self, connection, table_name, schema=None):

        if schema is None:
            schema=self.default_schema_name

        stmt = select([column('tablename')],
                      from_obj=[text('dbc.tablesvx')]).where(
                          and_(text('creatorname=:user'),
                               text('tablename=:name')))
        res = connection.execute(stmt, user=schema, name=table_name).fetchone()
        return res is not None

    def _get_default_schema_name(self, connection):
        return self.normalize_name(
            connection.execute('select user').scalar())

    def get_table_names(self, connection, schema=None, **kw):

        if schema is None:
            schema = self.default_schema_name

        stmt = select([column('tablename')],
                from_obj = [text('dbc.TablesVX')]).where(
                and_(text('creatorname = :user'),
                    or_(text('tablekind=\'T\''),
                        text('tablekind=\'O\''))))
        res = connection.execute(stmt, user=schema).fetchall()
        return [self.normalize_name(name['tablename']) for name in res]

    def get_schema_names(self, connection, **kw):
        stmt = select(['username'],
               from_obj=[text('dbc.UsersV')],
               order_by=['username'])
        res = connection.execute(stmt).fetchall()
        return [self.normalize_name(name['tablename']) for name in res]

    def get_view_names(self, connection, schema=None, **kw):
        stmt = select(['tablename'],
               from_obj=[text('dbc.TablesVX')],
               whereclause='tablekind=\'V\'')
        res = connection.execute(stmt).fetchall()
        return [self.normalize_name(name['tablename']) for name in res]

dialect = TeradataDialect
