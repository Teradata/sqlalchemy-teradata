from sqlalchemy_teradata.compiler import TeradataTypeCompiler as tdtc
from sqlalchemy_teradata.dialect import TeradataDialect as tdd
from sqlalchemy import create_engine, testing
from sqlalchemy.testing import fixtures
from sqlalchemy.sql import table, column, select

class TestCompileTDLimitOffset(fixtures.TestBase):
    """
    Test compilation of limit offset in Teradata
    """
    def setup(self):
        # Running this locally for now
        def dump(sql, *multiparams, **params):
            sql.compile(dialect = self.engine.tdd)

        self.engine = create_engine('teradata://', strategy='mock', executor=dump)

    def test_limit_offset(self):
        t1 = table('t1', column('c1'), column('c2'), column('c3'))
        s = select([t1]).limit(3).offset(5)
        #assert s ==
        s = select([t1]).limit(3)
        #assert s ==
        s = select([t1]).limit(3).distinct()
        #assert s ==
        s = select([t1]).order_by(t1.c.c2).limit(3).offset(5).distinct()
        #assert s ==
        s = select([t1]).order_by(t1.c.c2).limit(3).offset(5)
        #assert s ==
        s = select([t1]).order_by(t1.c.c2).offset(5)
        #assert s ==
        s = select([t1]).order_by(t1.c.c2).limit(3)
        #assert s ==
        stmt = s.compile(self.engine)

