# sqlalchemy_teradata/base.py
# Copyright (C) 2015-2016 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy.sql import compiler
from sqlalchemy.engine import default
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement, Executable
from sqlalchemy.schema import DDLElement
from sqlalchemy import types as sqltypes
from sqlalchemy.types import CHAR, DATE, DATETIME, \
                    BLOB, CLOB, TIMESTAMP, FLOAT, BIGINT, DECIMAL, NUMERIC, \
                    NCHAR, NVARCHAR, INTEGER, \
                    SMALLINT, TIME, TEXT, VARCHAR, REAL

ReservedWords = set(["abort", "abortsession", "abs", "access_lock", "account",
                    "acos", "acosh", "add", "add_months", "admin", "after",
                    "aggregate","all", "alter", "amp", "and", "ansidate",
                    "any", "arglparen", "as", "asc", "asin", "asinh", "at",
                     "atan", "atan2", "atanh", "atomic", "authorization", "ave",
                     "average", "avg", "before", "begin" , "between", "bigint",
                     "binary", "blob", "both", "bt", "but", "by", "byte", "byteint",
                     "bytes", "call", "case", "case_n", "casespecific", "cast", "cd",
                     "char", "char_length", "char2hexint","count", "title", "value",
                     'user','password',"year", "match"])

class TeradataExecutionContext(default.DefaultExecutionContext):

    def __init__(self, dialect, connection, dbapi_connection, compiled_ddl):

        super(TeradataExecutionContext, self).__init__(dialect, connection,
                                                       dbapi_connection,
                                                       compiled_ddl)

class TeradataIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = ReservedWords

    def __init__(self, dialect, initial_quote='"', final_quote=None, escape_quote='"', omit_schema=False):

        super(TeradataIdentifierPreparer, self).__init__(dialect, initial_quote, final_quote,
                                                         escape_quote, omit_schema)

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
