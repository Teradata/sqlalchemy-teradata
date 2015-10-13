from sqlalchemy.engine import default
from sqlalchemy import pool
from sqlalchemy_teradata.base import TeradataCompiler, TeradataDDLCompiler, TeradataIdentifierPreparer, TeradataExecutionContext

class TeradataDialect(default.DefaultDialect):
    name = 'tdalchemy'
    driver = 'teradata'  # gets imported in dbapi

    paramstyle = 'qmark'

    poolclass = pool.SingletonThreadPool
    statement_compiler = TeradataCompiler
    ddl_compiler = TeradataDDLCompiler
    preparer = TeradataIdentifierPreparer
    execution_ctx_cls = TeradataExecutionContext

    def __init__(self, **kwargs):
        super(TeradataDialect, self).__init__(**kwargs)

    # the hook to the dbapi 2.0 implementation (expects a connect method defined)
    @classmethod
    def dbapi(cls):
        from teradata import tdodbc
        return tdodbc

    # given a url object, returns tuple of *args or **kwargs to send directly to the dbapi's connect function
    def create_connect_args(self, url):
        if url is not None:
            params = url.translate_connect_args()
            print params
            return (("Teradata", params['host'], params['username'], params['password']), {})
        else:
            # todo: throw appropriate error
            pass
