"""
Support for Teradata

"""
from sqlalchemy.sql import compiler
from sqlalchemy.engine import default

ReservedWords = set(["ACCOUNT", "ALL", "ALLOW", "AND", "ANSIDATE",
                    "APPLY", "ARRAY", "AS", "ATTR", "ATTRIBUTES"])


class TeradataExecutionContext(default.DefaultExecutionContext):

    def __init__(self, dialect, connection, dbapi_connection, compiled_ddl):

        super(TeradataExecutionContext, self).__init__(dialect, connection,
                                                       dbapi_connection,
                                                       compiled_ddl)


class TeradataIdentifierPreparer(compiler.IdentifierPreparer):

    def __init__(self, dialect, initial_quote="", final_quote=None, escape_quote="", omit_schema=False):

        super(TeradataIdentifierPreparer, self).__init__(dialect, initial_quote, final_quote,
                                                         escape_quote, omit_schema)
