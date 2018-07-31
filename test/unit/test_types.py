from sqlalchemy import Table, Column
from sqlalchemy import MetaData, create_engine
from sqlalchemy import sql
from sqlalchemy_teradata import *
from sqlalchemy.sql import sqltypes
from sqlalchemy.testing import fixtures
from sqlalchemy.types import (BigInteger, Boolean, Date, DateTime, Enum, Float,
                              Integer, Interval, LargeBinary, Numeric,
                              SmallInteger, String, Text, Time, Unicode,
                              UnicodeText)
from sqlalchemy.testing.plugin.pytestplugin import *
from sqlalchemy_teradata.compiler import TeradataTypeCompiler
from sqlalchemy_teradata.dialect import TeradataDialect

import datetime as dt
import enum

"""
Unit testing for compiling Generic, SQL Standard, and Teradata data types.
"""

class TestCompileGeneric(fixtures.TestBase):

    def _compile(self, inst):
        return self.tdtc.process(inst)

    def setup(self):
        def dump(sql, *multiparams, **params):
            self.last_compiled = str(sql.compile(dialect=self.td_engine.dialect))
        self.td_engine = create_engine('teradata://', strategy='mock', executor=dump)
        self.metadata  = MetaData(bind=self.td_engine)

        self.tdtc = TeradataTypeCompiler(dialect=TeradataDialect())

    def test_compile_default(self):
        """
        Test compiling Generic types with default parameters.
        """

        assert(self._compile(BigInteger())   == 'BIGINT')
        assert(self._compile(Boolean())      == 'BYTEINT')
        assert(self._compile(Date())         == 'DATE')
        assert(self._compile(DateTime())     == 'TIMESTAMP(6)')
        assert(self._compile(Enum())         == 'VARCHAR(0)')
        assert(self._compile(Float())        == 'FLOAT')
        assert(self._compile(Integer())      == 'INTEGER')
        assert(self._compile(Interval())     == 'TIMESTAMP(6)')
        assert(self._compile(LargeBinary())  == 'BLOB')
        assert(self._compile(Numeric())      == 'NUMERIC')
        assert(self._compile(SmallInteger()) == 'SMALLINT')
        assert(self._compile(String())       == 'LONG VARCHAR')
        assert(self._compile(Text())         == 'CLOB')
        assert(self._compile(Time())         == 'TIME(6)')
        assert(self._compile(Unicode())      == 'LONG VARCHAR CHAR SET UNICODE')
        assert(self._compile(UnicodeText())  == 'CLOB CHAR SET UNICODE')

    def test_compile_create_table(self):
        """
        Tests that the generic types are compiled correctly. This test ensures
        that the DDL emitted by the generics translate to the correct corresponding
        database data types and with the necessary contraints when applicable
        (Boolean and non-native Enum).
        """

        class TestEnum(enum.Enum):
            one   = 1
            two   = 2
            three = 3

        col_types = {
            'column_0':  sqltypes.BigInteger(),
            'column_1':  sqltypes.Boolean(),
            'column_2':  sqltypes.Date(),
            'column_3':  sqltypes.DateTime(),
            'column_4':  sqltypes.Enum(TestEnum),
            'column_5':  sqltypes.Float(),
            'column_6':  sqltypes.Integer(),
            'column_7':  sqltypes.Interval(),
            'column_8':  sqltypes.LargeBinary(),
            'column_9':  sqltypes.Numeric(),
            'column_10': sqltypes.SmallInteger(),
            'column_11': sqltypes.String(),
            'column_12': sqltypes.Text(),
            'column_13': sqltypes.Time(),
            'column_14': sqltypes.Unicode(),
            'column_15': sqltypes.UnicodeText()
        }

        cols  = [Column(name, type) for name, type in col_types.items()]
        table = Table('table_test_types_generic', self.metadata, *cols)
        self.metadata.create_all(checkfirst=False)

        assert(self.last_compiled ==
            "\nCREATE TABLE table_test_types_generic ("
                "\n\tcolumn_0 BIGINT, "
                "\n\tcolumn_1 BYTEINT, "
                "\n\tcolumn_2 DATE, "
                "\n\tcolumn_3 TIMESTAMP(6), "
                "\n\tcolumn_4 VARCHAR(5), "
                "\n\tcolumn_5 FLOAT, "
                "\n\tcolumn_6 INTEGER, "
                "\n\tcolumn_7 TIMESTAMP(6), "
                "\n\tcolumn_8 BLOB, "
                "\n\tcolumn_9 NUMERIC, "
                "\n\tcolumn_10 SMALLINT, "
                "\n\tcolumn_11 LONG VARCHAR, "
                "\n\tcolumn_12 CLOB, "
                "\n\tcolumn_13 TIME(6), "
                "\n\tcolumn_14 LONG VARCHAR CHAR SET UNICODE, "
                "\n\tcolumn_15 CLOB CHAR SET UNICODE, "
                "\n\tCHECK (column_1 IN (0, 1)), "
                "\n\tCONSTRAINT testenum CHECK (column_4 IN ('one', 'two', 'three'))\n)"
            "\n\n")


class TestCompileSQLStandard(fixtures.TestBase):

    def _compile(self, inst):
        return self.tdtc.process(inst)

    def setup(self):
        self.tdtc = TeradataTypeCompiler(dialect=TeradataDialect())

    def test_compile_default(self):
        """
        Test compiling all SQL Standard types with default paramaters.
        """

        assert(self._compile(INTEGER())  == 'INTEGER')
        assert(self._compile(SMALLINT()) == 'SMALLINT')
        assert(self._compile(BIGINT())   == 'BIGINT')
        assert(self._compile(DECIMAL())  == 'DECIMAL')
        assert(self._compile(FLOAT())    == 'FLOAT')
        assert(self._compile(DATE())     == 'DATE')


class TestCompileTDTypes(fixtures.TestBase):

    """
    The tests are based of the info in SQL Data Types and Literals (Release
    15.10, Dec '15)
    """

    def _compile(self, inst):
        return self.tdtc.process(inst)

    def setup(self):
        self.tdtc = TeradataTypeCompiler(dialect=TeradataDialect)

        self.charsets  = ['latin, unicode, graphic, kanjisjis']
        self.lengths   = [-1, 32000, 64000]
        self.multiples = ['K', 'M', 'G']

    def test_compile_default(self):
        """
        Test compiling all Teradata types with default paramaters.
        """

        assert(self._compile(TIME())                      == 'TIME(6)')
        assert(self._compile(TIMESTAMP())                 == 'TIMESTAMP(6)')
        assert(self._compile(CHAR())                      == 'CHAR(1)')
        assert(self._compile(VARCHAR())                   == 'LONG VARCHAR')
        assert(self._compile(CLOB())                      == 'CLOB')
        assert(self._compile(NUMBER())                    == 'NUMBER')
        assert(self._compile(BYTEINT())                   == 'BYTEINT')
        assert(self._compile(BYTE())                      == 'BYTE')
        assert(self._compile(VARBYTE())                   == 'VARBYTE')
        assert(self._compile(BLOB())                      == 'BLOB')
        assert(self._compile(INTERVAL_YEAR())             == 'INTERVAL YEAR')
        assert(self._compile(INTERVAL_YEAR_TO_MONTH())    == 'INTERVAL YEAR TO MONTH')
        assert(self._compile(INTERVAL_MONTH())            == 'INTERVAL MONTH')
        assert(self._compile(INTERVAL_DAY())              == 'INTERVAL DAY')
        assert(self._compile(INTERVAL_DAY_TO_HOUR())      == 'INTERVAL DAY TO HOUR')
        assert(self._compile(INTERVAL_DAY_TO_MINUTE())    == 'INTERVAL DAY TO MINUTE')
        assert(self._compile(INTERVAL_DAY_TO_SECOND())    == 'INTERVAL DAY TO SECOND')
        assert(self._compile(INTERVAL_HOUR())             == 'INTERVAL HOUR')
        assert(self._compile(INTERVAL_HOUR_TO_MINUTE())   == 'INTERVAL HOUR TO MINUTE')
        assert(self._compile(INTERVAL_HOUR_TO_SECOND())   == 'INTERVAL HOUR TO SECOND')
        assert(self._compile(INTERVAL_MINUTE())           == 'INTERVAL MINUTE')
        assert(self._compile(INTERVAL_MINUTE_TO_SECOND()) == 'INTERVAL MINUTE TO SECOND')
        assert(self._compile(INTERVAL_SECOND())           == 'INTERVAL SECOND')
        assert(self._compile(PERIOD_DATE())               == 'PERIOD(DATE)')
        assert(self._compile(PERIOD_TIME())               == 'PERIOD(TIME)')
        assert(self._compile(PERIOD_TIMESTAMP())          == 'PERIOD(TIMESTAMP)')

    def test_compile_character(self):
        """
        Test compiling Teradata Character types (CHAR, VARCHAR, CLOB) with
        various parameters.
        """

        for m in self.multiples:
            c = CLOB(length=1, multiplier=m)
            assert(self._compile(c) == 'CLOB(1{})'.format(m))
            assert(c.length == 1)

        for len_ in self.lengths:
            assert('VARCHAR({})'.format(len_) == self._compile(VARCHAR(len_)))
            assert('CHAR({})'.format(len_)    == self._compile(CHAR(len_)))
            assert('CLOB({})'.format(len_)    == self._compile(CLOB(len_)))

            for c in self.charsets:
                assert('VARCHAR({}) CHAR SET {}'.format(len_, c) ==
                    self._compile(VARCHAR(len_, c)))

                assert('CHAR({}) CHAR SET {}'.format(len_, c) ==
                    self._compile(CHAR(len_, c)))

                assert('CLOB({}) CHAR SET {}'.format(len_, c) ==
                    self._compile(CLOB(len_, c)))

    def test_compile_datetime_timezones(self):
        """
        Test compiling Teradata datetime types with timezone.
        """

        assert(self._compile(TIME(1, True))      == 'TIME(1) WITH TIME ZONE')
        assert(self._compile(TIMESTAMP(0, True)) == 'TIMESTAMP(0) WITH TIME ZONE')

    def test_compile_interval_prec(self):
        """
        Test valid ranges of precision (prec) for Teradata Interval types.
        """

        for prec in range(1,5):
            assert(self._compile(INTERVAL_YEAR(prec)) ==
                'INTERVAL YEAR({})'.format(prec))
            assert(self._compile(INTERVAL_YEAR_TO_MONTH(prec)) ==
                'INTERVAL YEAR({}) TO MONTH'.format(prec))
            assert(self._compile(INTERVAL_MONTH(prec)) ==
                'INTERVAL MONTH({})'.format(prec))
            assert(self._compile(INTERVAL_DAY(prec)) ==
                'INTERVAL DAY({})'.format(prec))
            assert(self._compile(INTERVAL_DAY_TO_HOUR(prec)) ==
                'INTERVAL DAY({}) TO HOUR'.format(prec))
            assert(self._compile(INTERVAL_DAY_TO_MINUTE(prec)) ==
                'INTERVAL DAY({}) TO MINUTE'.format(prec))
            assert(self._compile(INTERVAL_HOUR(prec)) ==
                'INTERVAL HOUR({})'.format(prec))
            assert(self._compile(INTERVAL_HOUR_TO_MINUTE(prec)) ==
                'INTERVAL HOUR({}) TO MINUTE'.format(prec))
            assert(self._compile(INTERVAL_MINUTE(prec)) ==
                'INTERVAL MINUTE({})'.format(prec))
            assert(self._compile(INTERVAL_SECOND(prec)) ==
                'INTERVAL SECOND({})'.format(prec))

    def test_compile_interval_frac(self):
        """
        Test valid ranges of precision (prec) and fractional second precision
        (fsec) for Teradata Interval types.
        """

        for prec in range(1,5):
            for fsec in range(0,7):
                assert(self._compile(INTERVAL_DAY_TO_SECOND(prec, fsec)) ==
                    'INTERVAL DAY({}) TO SECOND({})'.format(prec, fsec))

                assert(self._compile(INTERVAL_HOUR_TO_SECOND(prec, fsec)) ==
                    'INTERVAL HOUR({}) TO SECOND({})'.format(prec, fsec))

                assert(self._compile(INTERVAL_MINUTE_TO_SECOND(prec, fsec)) ==
                    'INTERVAL MINUTE({}) TO SECOND({})'.format(prec, fsec))

                assert(self._compile(INTERVAL_SECOND(prec, fsec)) ==
                    'INTERVAL SECOND({}, {})'.format(prec, fsec))

class TestStrTDTypes(fixtures.TestBase):

    def test_str_default(self):
        """
        Test printing out (calling str) on each of the data types implemented
        in types.py.
        """

        assert(str(TIME())                      == 'TIME')
        assert(str(TIMESTAMP())                 == 'TIMESTAMP')
        assert(str(CHAR())                      == 'CHAR')
        assert(str(VARCHAR())                   == 'VARCHAR')
        assert(str(CLOB())                      == 'CLOB')
        assert(str(NUMBER())                    == 'NUMBER')
        assert(str(BYTEINT())                   == 'BYTEINT')
        assert(str(BYTE())                      == 'BYTE')
        assert(str(VARBYTE())                   == 'VARBYTE')
        assert(str(BLOB())                      == 'BLOB')
        assert(str(INTERVAL_YEAR())             == 'INTERVAL YEAR')
        assert(str(INTERVAL_YEAR_TO_MONTH())    == 'INTERVAL YEAR TO MONTH')
        assert(str(INTERVAL_MONTH())            == 'INTERVAL MONTH')
        assert(str(INTERVAL_DAY())              == 'INTERVAL DAY')
        assert(str(INTERVAL_DAY_TO_HOUR())      == 'INTERVAL DAY TO HOUR')
        assert(str(INTERVAL_DAY_TO_MINUTE())    == 'INTERVAL DAY TO MINUTE')
        assert(str(INTERVAL_DAY_TO_SECOND())    == 'INTERVAL DAY TO SECOND')
        assert(str(INTERVAL_HOUR())             == 'INTERVAL HOUR')
        assert(str(INTERVAL_HOUR_TO_MINUTE())   == 'INTERVAL HOUR TO MINUTE')
        assert(str(INTERVAL_HOUR_TO_SECOND())   == 'INTERVAL HOUR TO SECOND')
        assert(str(INTERVAL_MINUTE())           == 'INTERVAL MINUTE')
        assert(str(INTERVAL_MINUTE_TO_SECOND()) == 'INTERVAL MINUTE TO SECOND')
        assert(str(INTERVAL_SECOND())           == 'INTERVAL SECOND')
        assert(str(PERIOD_DATE())               == 'PERIOD DATE')
        assert(str(PERIOD_TIME())               == 'PERIOD TIME')
        assert(str(PERIOD_TIMESTAMP())          == 'PERIOD TIMESTAMP')
