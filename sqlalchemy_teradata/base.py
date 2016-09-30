# sqlalchemy_teradata/base.py
# Copyright (C) 2015-2016 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import *
from sqlalchemy.sql import compiler
from sqlalchemy.engine import default
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement, Executable
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import table
from sqlalchemy import types as sqltypes
from sqlalchemy.types import CHAR, DATE, DATETIME, \
                    BLOB, CLOB, TIMESTAMP, FLOAT, BIGINT, DECIMAL, NUMERIC, \
                    NCHAR, NVARCHAR, INTEGER, \
                    SMALLINT, TIME, TEXT, VARCHAR, REAL

#TODO: Read this from the dbc.restrictedwordsv view
ReservedWords = set(["abort", "abortsession", "abs", "access_lock", "account",
                    "acos", "acosh", "add", "add_months", "admin", "after",
                    "aggregate","all", "alter", "amp", "and", "ansidate",
                    "any", "arglparen", "as", "asc", "asin", "asinh", "at",
                     "atan", "atan2", "atanh", "atomic", "authorization", "ave",
                     "average", "avg", "before", "begin" , "between", "bigint",
                     "binary", "blob", "both", "bt", "but", "by", "byte", "byteint",
                     "bytes", "call", "case", "case_n", "casespecific", "cast", "cd",
                     "char", "char_length", "char2hexint", "count","day", "desc", "hour",
                     "in", "le", "minute", "meets", "month", "order", "ordering",
                     "title", "value",
                     'user','password', "preceded", "second", "succeeds", "year", "match", "time", "timestamp"])

class TeradataExecutionContext(default.DefaultExecutionContext):

    def __init__(self, dialect, connection, dbapi_connection, compiled_ddl):
        super(TeradataExecutionContext, self).__init__(dialect, connection, dbapi_connection, compiled_ddl)

class TeradataIdentifierPreparer(compiler.IdentifierPreparer):

    reserved_words = ReservedWords

    def __init__(self, dialect, initial_quote='"', final_quote=None, escape_quote='"', omit_schema=False):

        super(TeradataIdentifierPreparer, self).__init__(dialect, initial_quote, final_quote,
                                                         escape_quote, omit_schema)

# Views Recipe from: https://bitbucket.org/zzzeek/sqlalchemy/wiki/UsageRecipes/Views
class CreateView(DDLElement):

    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable

class DropView(DDLElement):

    def __init__(self, name):
        self.name = name

@compiles(CreateView)
def visit_create_view(element, compiler, **kw):
    return "CREATE VIEW {} AS {}".format(element.name, compiler.sql_compiler.process(element.selectable))

@compiles(DropView)
def visit_drop_view(element, compiler, **kw):
    return "DROP VIEW {}".format(element.name)

class CreateTableAs(DDLElement):
        pass

@compiles(CreateTableAs)
def visit_create_table(element, table, **kw):
        pass

class CreateTableQueue(DDLElement):
        pass

class CreateTableGlobalTempTrace(DDLElement):
        pass
class CreateErrorTable(DDLElement):
        pass

class IdentityColumn(DDLElement):
        pass

class CreateJoinIndex():
    pass

class CreateHashIndex():
    pass
