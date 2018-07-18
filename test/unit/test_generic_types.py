from sqlalchemy_teradata.compiler import TeradataTypeCompiler as tdtc
from sqlalchemy_teradata.dialect import TeradataDialect as tdd
from sqlalchemy.types import (Integer, SmallInteger, BigInteger, Numeric,
                Float, DateTime, Date, String, Text, Unicode, UnicodeText,
                Time, LargeBinary, Boolean, Interval,
                DATE, BOOLEAN, DATETIME, BIGINT, SMALLINT, INTEGER, FLOAT, REAL,
                TEXT, NVARCHAR, NCHAR)
from sqlalchemy_teradata.types import (CHAR, VARCHAR, CLOB, DECIMAL, NUMERIC,
                                       VARCHAR, TIMESTAMP, TIME)
from sqlalchemy.testing import fixtures

from itertools import product
import datetime as dt

class TestCompileGeneric(fixtures.TestBase):

   def _comp(self, inst):
       return self.comp.process(inst)

   def setup(self):
       # Teradata Type Compiler using Teradata Dialect to compile types
       self.comp = tdtc(tdd)
       self.charset= ['latin, unicode, graphic, kanjisjis']
       self.len_limits = [-1, 32000, 64000]
       self.multips = ['K', 'M', 'G']

   def test_defaults(self):
       assert self._comp(Integer()) == 'INTEGER'
       assert self._comp(SmallInteger()) == 'SMALLINT'
       assert self._comp(BigInteger()) == 'BIGINT'
       assert self._comp(Numeric()) == 'NUMERIC'
       assert self._comp(Float()) == 'FLOAT'

       assert self._comp(DateTime()) == 'TIMESTAMP(6)'
       assert self._comp(Date()) == 'DATE'
       assert self._comp(Time()) == 'TIME(6)'

       assert self._comp(String()) == 'LONG VARCHAR'
       assert self._comp(Text()) == 'CLOB'
       assert self._comp(Unicode()) == 'LONG VARCHAR CHAR SET UNICODE'
       assert self._comp(UnicodeText()) == 'CLOB CHAR SET UNICODE'

       assert self._comp(Boolean()) ==  'BYTEINT'
       #assert self._comp(LargeBinary()) == 'BLOB'

class TestCompileSQLStandard(fixtures.TestBase):

    def _comp(self, inst):
        return self.comp.process(inst)

    def setup(self):
        self.comp = tdtc(tdd)

    def test_defaults(self):
        assert self._comp(DATE()) == 'DATE'
        assert self._comp(DATETIME()) == 'TIMESTAMP(6)'
        assert self._comp(TIMESTAMP()) == 'TIMESTAMP(6)'
        assert self._comp(TIME()) == 'TIME(6)'

        assert self._comp(CHAR()) == 'CHAR(1)'
        assert self._comp(VARCHAR()) == 'LONG VARCHAR'
        assert self._comp(NCHAR()) == 'CHAR CHAR SET UNICODE'
        assert self._comp(NVARCHAR()) == 'LONG VARCHAR CHAR SET UNICODE'
        assert self._comp(CLOB()) == 'CLOB'
        assert self._comp(TEXT()) ==  'CLOB'

        assert self._comp(DECIMAL()) == 'DECIMAL(5, 0)'
        assert self._comp(NUMERIC()) == 'NUMERIC(5, 0)'
        assert self._comp(INTEGER()) ==  'INTEGER'
        assert self._comp(FLOAT()) ==  'FLOAT'
        assert self._comp(REAL()) ==  'REAL'
        assert self._comp(SMALLINT()) ==  'SMALLINT'
        assert self._comp(BIGINT()) ==  'BIGINT'

        assert self._comp(BOOLEAN()) == 'BYTEINT'


class TestCompileTypes(fixtures.TestBase):

    """
    The tests are based of the info in SQL Data Types and Literals (Release 15.10, Dec '15)
    """
    def setup(self):
        self.comp = tdtc(tdd)
        self.charset= ['latin, unicode, graphic, kanjisjis']
        self.len_limits = [-1, 32000, 64000]
        self.multips = ['K', 'M', 'G']

    def test_strings(self):

        for m in self.multips:
            c = CLOB(length = 1, multiplier = m)
            assert self.comp.process(c) == 'CLOB(1{})'.format(m)
            assert c.length == 1

        for len_ in self.len_limits:
            assert 'VARCHAR({})'.format(len_) == self.comp.process(VARCHAR(len_))
            assert 'CHAR({})'.format(len_) == self.comp.process(CHAR(len_))
            assert 'CLOB({})'.format(len_) == self.comp.process(CLOB(len_))

            for c in self.charset:
               assert 'VARCHAR({}) CHAR SET {}'.format(len_, c) == \
                               self.comp.process(VARCHAR(len_, c))

               assert 'CHAR({}) CHAR SET {}'.format(len_, c) == \
                               self.comp.process(CHAR(len_, c))

               assert 'CLOB({}) CHAR SET {}'.format(len_, c) == \
                               self.comp.process(CLOB(len_, c))

    def test_timezones(self):
        assert self.comp.process(TIME(1, True)) == 'TIME(1) WITH TIME ZONE'
        assert self.comp.process(TIMESTAMP(0, True)) == 'TIMESTAMP(0) WITH TIME ZONE'
