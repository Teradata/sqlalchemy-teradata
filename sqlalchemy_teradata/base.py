# sqlalchemy_teradata/base.py
# Copyright (C) 2015-2019 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import re

from sqlalchemy.engine import default
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DDLElement
from sqlalchemy.sql import compiler
from .restricted_words import restricted_words


AUTOCOMMIT_REGEXP = re.compile(
    r'\s*(?:UPDATE|INSERT|CREATE|DELETE|DROP|ALTER|MERGE)',
    re.I | re.UNICODE)

RESERVED_WORDS = restricted_words


class TeradataExecutionContext(default.DefaultExecutionContext):

    def __init__(self, dialect, connection, dbapi_connection, compiled_ddl):
        super(TeradataExecutionContext, self).__init__(
            dialect, connection, dbapi_connection, compiled_ddl)

    def should_autocommit_text(self, statement):
        return AUTOCOMMIT_REGEXP.match(statement)


class TeradataIdentifierPreparer(compiler.IdentifierPreparer):

    reserved_words = RESERVED_WORDS

    def __init__(self, dialect, initial_quote='"', final_quote=None,
                 escape_quote='"', omit_schema=False):
        super(TeradataIdentifierPreparer, self).__init__(
            dialect, initial_quote, final_quote, escape_quote, omit_schema)


# Views Recipe from:
# https://bitbucket.org/zzzeek/sqlalchemy/wiki/UsageRecipes/Views
class CreateView(DDLElement):

    def __init__(self, name, selectable):
        self.name = name
        self.selectable = selectable

@compiles(CreateView)
def visit_create_view(element, compiler, **kw):
    return 'CREATE VIEW {} AS {}'.format(
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True))


class DropView(DDLElement):

    def __init__(self, name):
        self.name = name

@compiles(DropView)
def visit_drop_view(element, compiler, **kw):
    return "DROP VIEW {}".format(element.name)


class CreateTableAs(DDLElement):

    def __init__(self, name, selectable, data=False):
        self.name = name
        self.selectable = selectable
        self.data = data

@compiles(CreateTableAs)
def visit_create_table_as(element, compiler, **kw):
    return 'CREATE TABLE {} AS ({}) WITH{}DATA;'.format(
        element.name,
        compiler.sql_compiler.process(element.selectable, literal_binds=True),
        ' ' if element.data else ' NO ')


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
