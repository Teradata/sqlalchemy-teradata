from sqlalchemy.engine import default
from sqlalchemy import pool
from sqlalchemy_teradata.compiler import TeradataCompiler, TeradataDDLCompiler
from sqlalchemy_teradata.base import TeradataIdentifierPreparer, TeradataExecutionContext


class TeradataDialect(default.DefaultDialect):
    """
    Implements the Dialect interface. TeradataDialect inherits from the
       default.DefaultDialect. Changes made here are specific to Teradata where
       the default implementation isn't sufficient.

       Note that the default.DefaultDialect delegates some methods to the OdbcConnection
       in the tdodbc module passed in the dbapi class method

       """

    name = 'tdalchemy'
    driver = 'teradata'

    poolclass = pool.SingletonThreadPool
    statement_compiler = TeradataCompiler
    ddl_compiler = TeradataDDLCompiler
    preparer = TeradataIdentifierPreparer
    execution_ctx_cls = TeradataExecutionContext

    def __init__(self, **kwargs):
        super(TeradataDialect, self).__init__(**kwargs)

    def create_connect_args(self, url):
        if url is not None:
            params = super(TeradataDialect, self).create_connect_args(url)[1]
            return (("Teradata", params['host'], params['username'], params['password']), {})

    @classmethod
    def dbapi(cls):
        """ Hook to the dbapi2.0 implementation's module"""
        from teradata import tdodbc
        return tdodbc

    def has_table(self, connection, table_name, schema=None):
        q = 'select count(*) from dbc.tables where tablename=\'{}\''.format(table_name)
        res = connection.execute(q)
        row = res.fetchone()

        #count is 0 or >0
        return row[0]


    # Helper functions
    def get_dbc_table_names(self, connection):
        """Return a list of table names for `dbc`."""
        pass

    def grant_privilege(self, privs, on_obj, to_obj, wgo=False):
        """Grant privs on on_obj to to_obj [wgo]"""
        pass
