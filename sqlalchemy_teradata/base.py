"""
Support for Teradata


"""
from sqlalchemy import sql, schema, types, exc, pool
from sqlalchemy.sql import compiler, expression
from sqlalchemy.engine import default, base, reflection
from sqlalchemy import processors

#class AcNumeric(types.Numeric):
#    def get_col_spec(self):
#        return "NUMERIC"
#
#    def bind_processor(self, dialect):
#        return processors.to_str
#
#    def result_processor(self, dialect, coltype):
#        return None

class TeradataExecutionContext(default.DefaultExecutionContext):
    pass

#    def get_lastrowid(self):
#        self.cursor.execute("SELECT @@identity AS lastrowid")
#        return self.cursor.fetchone()[0]


class TeradataCompiler(compiler.SQLCompiler):
    pass
    #def visit_select_precolumns(self, select):
    #    """Access puts TOP, it's version of LIMIT here """
    #    s = select.distinct and "DISTINCT " or ""
    #    if select.limit:
    #        s += "TOP %s " % (select.limit)
    #    if select.offset:
    #        raise exc.InvalidRequestError(
    #            'Access does not support LIMIT with an offset')
    #    return s

    #function_rewrites = {'current_date': 'now',
    #                      'current_timestamp': 'now',
    #                      'length': 'len',
    #                      }
    #def visit_function(self, func, **kwargs):
    #    """Access function names differ from the ANSI SQL names;
    #    rewrite common ones"""
    #    func.name = self.function_rewrites.get(func.name, func.name)
    #    return super(AccessCompiler, self).visit_function(func)

class TeradataIdentifierPreparer(compiler.IdentifierPreparer):
    pass
    #reserved_words = compiler.RESERVED_WORDS.copy()
    #reserved_words.update(['value', 'text'])
    #def __init__(self, dialect):
    #    super(AccessIdentifierPreparer, self).\
    #            __init__(dialect, initial_quote='[', final_quote=']')

class TeradataDDLCompiler(compiler.DDLCompiler):
    pass
#    def visit_drop_index(self, drop):
#        index = drop.element
#        self.append("\nDROP INDEX [%s].[%s]" % \
#                        (index.table.name,
#                        self._index_identifier(index.name)))


#class TeradataDialect(default.DefaultDialect):
#    name = 'teradata-sqlalchemy'
#    driver = 'teradata'
#    #supports_sane_rowcount = False
#    #supports_sane_multi_rowcount = False
#
#    poolclass = pool.SingletonThreadPool
#    statement_compiler = TeradataCompiler
#    ddl_compiler = TeradataDDLCompiler
#    preparer = TeradataIdentifierPreparer
#    execution_ctx_cls = TeradataExecutionContext
#
#    @classmethod
#    def dbapi(cls):
#        import pyodbc as module
#        module.pooling = False
#        return module
#
#    #def last_inserted_ids(self):
    #    return self.context.last_inserted_ids

    #def has_table(self, connection, tablename, schema=None):
    #    result = connection.scalar(
    #                    sql.text(
    #                        "select count(*) from msysobjects where "
    #                        "type=1 and name=:name"), name=tablename
    #                    )
    #    return bool(result)

    #@reflection.cache
    #def get_table_names(self, connection, schema=None, **kw):
    #    result = connection.execute("select name from msysobjects where "
    #            "type=1 and name not like 'MSys%'")
    #    table_names = [r[0] for r in result]
    #    return table_names


