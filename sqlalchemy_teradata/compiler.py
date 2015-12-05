from sqlalchemy.sql import compiler


class TeradataCompiler(compiler.SQLCompiler):

    def __init__(self, dialect, statement, column_keys=None, inline=False, **kwargs):
        super(TeradataCompiler, self).__init__(dialect, statement, column_keys, inline, **kwargs)


class TeradataDDLCompiler(compiler.DDLCompiler):

    def visit_something(self, something):
        pass
