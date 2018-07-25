from sqlalchemy import *
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import testing
from sqlalchemy import sql
from sqlalchemy.sql import sqltypes
from sqlalchemy.engine import reflection
from sqlalchemy.testing.plugin.pytestplugin import *
from sqlalchemy_teradata.compiler import TDCreateTablePost, TDCreateTableSuffix
from test import utils

import decimal, datetime
import enum
import itertools
import sqlalchemy_teradata as sqlalch_td
import teradata.datatypes as td_dtypes

"""
Integration testing for DDL Expressions and Dialect Extensions
The tests are based of off SQL Data Definition Language (Release 15.10, Dec '15)
"""

class TestTypesDDLGeneral(testing.fixtures.TestBase):

    """
    This class conducts general testing of each of the data types supported by
    the Teradata dialect. Here, we're simply checking to ensure that each type
    gets reflected back properly, corresponds to the right Python type when
    queried, and emits the correct SHOW TABLE statement. Further testing of
    type attributes and data insertion are delegated to the TestTypesDDLDetailed
    class that follows.
    """

    def _setup_types(self):
        class TestEnum(enum.Enum):
            one   = 1
            two   = 2
            three = 3

        generic_types = (
            sqltypes.BigInteger(),
            sqltypes.Boolean(),
            sqltypes.Date(),
            sqltypes.DateTime(),
            sqltypes.Enum(TestEnum),
            sqltypes.Float(),
            sqltypes.Integer(),
            sqltypes.Interval(),
            sqltypes.LargeBinary(),
            sqltypes.Numeric(),
            sqltypes.SmallInteger(),
            sqltypes.String(),
            sqltypes.Text(),
            sqltypes.Time(),
            sqltypes.Unicode(),
            sqltypes.UnicodeText()
        )
        # Grab all the types from sqlalchemy_teradata.__all__ and VARBYTE needs
        # to be specifically instantiated with a length value
        td_types = tuple(type_(length=10) if type_ == sqlalch_td.VARBYTE
            else type_ for type_ in sqlalch_td.__all__)

        return generic_types + td_types

    def setup(self):
        self.conn     = testing.db.connect()
        self.engine   = self.conn.engine
        self.metadata = MetaData(bind=self.engine)
        self.inspect  = reflection.Inspector.from_engine(self.engine)

        self.sqlalch_types = self._setup_types()
        self.rawsql_types  = (
            'CHARACTER', 'VARCHAR(50)', 'CLOB', 'BIGINT', 'SMALLINT',
            'BYTEINT', 'INTEGER', 'DECIMAL', 'FLOAT', 'NUMBER',
            'DATE', 'TIME', 'TIMESTAMP'
        )

    def tearDown(self):
        self.metadata.drop_all(self.engine)
        self.conn.close()

    def test_types_sqlalch_select(self):
        """
        Tests that the SQLAlchemy types exported by sqlalchemy_teradata
        correctly translate to the corresponding native Python types through
        selection.

        This is carried out by creating a test table containing all the exported
        types and then querying data from that table to check that the returned
        cursor_description (types) matches expectation. The test table is
        created through sqlalchemy schema constructs and meta.create_all().
        """
        cols = [Column('column_' + str(i), type)
            for i, type in enumerate(self.sqlalch_types)]
        table = Table('table_test_types_sqlalch', self.metadata, *cols)
        self.metadata.create_all(checkfirst=False)

        col_to_type = {col.name: type(col.type) for col in cols}
        type_map    = {
            sqltypes.BigInteger:                  decimal.Decimal,
            sqltypes.Boolean:                     decimal.Decimal,
            sqltypes.Date:                        datetime.date,
            sqltypes.DateTime:                    datetime.datetime,
            sqltypes.Enum:                        str,
            sqltypes.Float:                       decimal.Decimal,
            sqltypes.Integer:                     decimal.Decimal,
            sqltypes.Interval:                    datetime.datetime,
            sqltypes.LargeBinary:                 bytearray,
            sqltypes.Numeric:                     decimal.Decimal,
            sqltypes.SmallInteger:                decimal.Decimal,
            sqltypes.String:                      str,
            sqltypes.Text:                        str,
            sqltypes.Time:                        datetime.time,
            sqltypes.Unicode:                     str,
            sqltypes.UnicodeText:                 str,

            sqlalch_td.INTEGER:                   decimal.Decimal,
            sqlalch_td.SMALLINT:                  decimal.Decimal,
            sqlalch_td.BIGINT:                    decimal.Decimal,
            sqlalch_td.DECIMAL:                   decimal.Decimal,
            sqlalch_td.FLOAT:                     decimal.Decimal,
            sqlalch_td.DATE:                      datetime.date,

            sqlalch_td.TIME:                      datetime.time,
            sqlalch_td.TIMESTAMP:                 datetime.datetime,
            sqlalch_td.CHAR:                      str,
            sqlalch_td.VARCHAR:                   str,
            sqlalch_td.CLOB:                      str,
            sqlalch_td.NUMBER:                    decimal.Decimal,
            sqlalch_td.BYTEINT:                   decimal.Decimal,
            sqlalch_td.BYTE:                      bytearray,
            sqlalch_td.VARBYTE:                   bytearray,
            sqlalch_td.BLOB:                      bytearray,
            sqlalch_td.INTERVAL_YEAR:             str,
            sqlalch_td.INTERVAL_YEAR_TO_MONTH:    str,
            sqlalch_td.INTERVAL_MONTH:            str,
            sqlalch_td.INTERVAL_DAY:              str,
            sqlalch_td.INTERVAL_DAY_TO_HOUR:      str,
            sqlalch_td.INTERVAL_DAY_TO_MINUTE:    str,
            sqlalch_td.INTERVAL_DAY_TO_SECOND:    str,
            sqlalch_td.INTERVAL_HOUR:             str,
            sqlalch_td.INTERVAL_HOUR_TO_MINUTE:   str,
            sqlalch_td.INTERVAL_HOUR_TO_SECOND:   str,
            sqlalch_td.INTERVAL_MINUTE:           str,
            sqlalch_td.INTERVAL_MINUTE_TO_SECOND: str,
            sqlalch_td.INTERVAL_SECOND:           str,
            sqlalch_td.PERIOD_DATE:               str,
            sqlalch_td.PERIOD_TIME:               str,
            sqlalch_td.PERIOD_TIMESTAMP:          str
        }

        res = self.conn.execute(table.select())
        for col in res._cursor_description():
            assert(type_map[col_to_type[col[0]]] == col[1])

    def test_types_sqlalch_reflect(self):
        """
        Tests that the SQLAlchemy types exported by sqlalchemy_teradata
        correctly translate to the corresponding SQLAlchemy types through
        table reflection.

        This is carried out by creating a test table containing all the exported
        types and then reflecting the table back and checking that each column
        type is consistent with the types the table was created with. The test
        table is created through sqlalchemy schema constructs and
        meta.create_all().
        """
        cols = [Column('column_' + str(i), type)
            for i, type in enumerate(self.sqlalch_types)]
        table = Table('table_test_types_sqlalch', self.metadata, *cols)
        self.metadata.create_all(checkfirst=False)

        col_to_type = {col.name: type(col.type) for col in cols}
        type_map    = {
            sqltypes.BigInteger:                  sqlalch_td.BIGINT,
            sqltypes.Boolean:                     sqlalch_td.BYTEINT,
            sqltypes.Date:                        sqlalch_td.DATE,
            sqltypes.DateTime:                    sqlalch_td.TIMESTAMP,
            sqltypes.Enum:                        sqlalch_td.VARCHAR,
            sqltypes.Float:                       sqlalch_td.FLOAT,
            sqltypes.Integer:                     sqlalch_td.INTEGER,
            sqltypes.Interval:                    sqlalch_td.TIMESTAMP,
            sqltypes.LargeBinary:                 sqlalch_td.BLOB,
            sqltypes.Numeric:                     sqlalch_td.DECIMAL,
            sqltypes.SmallInteger:                sqlalch_td.SMALLINT,
            sqltypes.String:                      sqlalch_td.VARCHAR,
            sqltypes.Text:                        sqlalch_td.CLOB,
            sqltypes.Time:                        sqlalch_td.TIME,
            sqltypes.Unicode:                     sqlalch_td.VARCHAR,
            sqltypes.UnicodeText:                 sqlalch_td.CLOB,

            sqlalch_td.INTEGER:                   sqlalch_td.INTEGER,
            sqlalch_td.SMALLINT:                  sqlalch_td.SMALLINT,
            sqlalch_td.BIGINT:                    sqlalch_td.BIGINT,
            sqlalch_td.DECIMAL:                   sqlalch_td.DECIMAL,
            sqlalch_td.FLOAT:                     sqlalch_td.FLOAT,
            sqlalch_td.DATE:                      sqlalch_td.DATE,

            sqlalch_td.TIME:                      sqlalch_td.TIME,
            sqlalch_td.TIMESTAMP:                 sqlalch_td.TIMESTAMP,
            sqlalch_td.CHAR:                      sqlalch_td.CHAR,
            sqlalch_td.VARCHAR:                   sqlalch_td.VARCHAR,
            sqlalch_td.CLOB:                      sqlalch_td.CLOB,
            sqlalch_td.NUMBER:                    sqlalch_td.NUMBER,
            sqlalch_td.BYTEINT:                   sqlalch_td.BYTEINT,
            sqlalch_td.BYTE:                      sqlalch_td.BYTE,
            sqlalch_td.VARBYTE:                   sqlalch_td.VARBYTE,
            sqlalch_td.BLOB:                      sqlalch_td.BLOB,
            sqlalch_td.INTERVAL_YEAR:             sqlalch_td.INTERVAL_YEAR,
            sqlalch_td.INTERVAL_YEAR_TO_MONTH:    sqlalch_td.INTERVAL_YEAR_TO_MONTH,
            sqlalch_td.INTERVAL_MONTH:            sqlalch_td.INTERVAL_MONTH,
            sqlalch_td.INTERVAL_DAY:              sqlalch_td.INTERVAL_DAY,
            sqlalch_td.INTERVAL_DAY_TO_HOUR:      sqlalch_td.INTERVAL_DAY_TO_HOUR,
            sqlalch_td.INTERVAL_DAY_TO_MINUTE:    sqlalch_td.INTERVAL_DAY_TO_MINUTE,
            sqlalch_td.INTERVAL_DAY_TO_SECOND:    sqlalch_td.INTERVAL_DAY_TO_SECOND,
            sqlalch_td.INTERVAL_HOUR:             sqlalch_td.INTERVAL_HOUR,
            sqlalch_td.INTERVAL_HOUR_TO_MINUTE:   sqlalch_td.INTERVAL_HOUR_TO_MINUTE,
            sqlalch_td.INTERVAL_HOUR_TO_SECOND:   sqlalch_td.INTERVAL_HOUR_TO_SECOND,
            sqlalch_td.INTERVAL_MINUTE:           sqlalch_td.INTERVAL_MINUTE,
            sqlalch_td.INTERVAL_MINUTE_TO_SECOND: sqlalch_td.INTERVAL_MINUTE_TO_SECOND,
            sqlalch_td.INTERVAL_SECOND:           sqlalch_td.INTERVAL_SECOND,
            sqlalch_td.PERIOD_DATE:               sqlalch_td.PERIOD_DATE,
            sqlalch_td.PERIOD_TIME:               sqlalch_td.PERIOD_TIME,
            sqlalch_td.PERIOD_TIMESTAMP:          sqlalch_td.PERIOD_TIMESTAMP
        }

        reflected_cols = self.inspect.get_columns('table_test_types_sqlalch')
        for col in reflected_cols:
            assert(type_map[col_to_type[col['name']]] == type(col['type']))

    def test_types_sqlalch_show(self):
        """
        Tests that the SQLAlchemy types exported by sqlalchemy_teradata
        correctly translate to the corresponding database types through
        show table.

        This is carried out by creating a test table containing all the exported
        types and then doing a SHOW TABLE query on the created table. The
        returned table string is then parsed to extract the attributes of each
        column and then checked against the expected types. The test table is
        created through sqlalchemy schema constructs and meta.create_all().
        """
        cols = [Column('column_' + str(i), type)
            for i, type in enumerate(self.sqlalch_types)]
        table = Table('table_test_types_sqlalch', self.metadata, *cols)
        self.metadata.create_all(checkfirst=False)

        col_to_type = {col.name: type(col.type) for col in cols}
        type_map    = {
            sqltypes.BigInteger:                  'BIGINT',
            sqltypes.Boolean:                     'BYTEINT',
            sqltypes.Date:                        'DATE',
            sqltypes.DateTime:                    'TIMESTAMP',
            sqltypes.Enum:                        'VARCHAR',
            sqltypes.Float:                       'FLOAT',
            sqltypes.Integer:                     'INTEGER',
            sqltypes.Interval:                    'TIMESTAMP',
            sqltypes.LargeBinary:                 'BLOB',
            sqltypes.Numeric:                     'DECIMAL',
            sqltypes.SmallInteger:                'SMALLINT',
            sqltypes.String:                      'VARCHAR',
            sqltypes.Text:                        'CLOB',
            sqltypes.Time:                        'TIME',
            sqltypes.Unicode:                     'VARCHAR',
            sqltypes.UnicodeText:                 'CLOB',

            sqlalch_td.INTEGER:                   'INTEGER',
            sqlalch_td.SMALLINT:                  'SMALLINT',
            sqlalch_td.BIGINT:                    'BIGINT',
            sqlalch_td.DECIMAL:                   'DECIMAL',
            sqlalch_td.FLOAT:                     'FLOAT',
            sqlalch_td.DATE:                      'DATE',

            sqlalch_td.TIME:                      'TIME',
            sqlalch_td.TIMESTAMP:                 'TIMESTAMP',
            sqlalch_td.CHAR:                      'CHAR',
            sqlalch_td.VARCHAR:                   'VARCHAR',
            sqlalch_td.CLOB:                      'CLOB',
            sqlalch_td.NUMBER:                    'NUMBER',
            sqlalch_td.BYTEINT:                   'BYTEINT',
            sqlalch_td.BYTE:                      'BYTE',
            sqlalch_td.VARBYTE:                   'VARBYTE',
            sqlalch_td.BLOB:                      'BLOB',
            sqlalch_td.INTERVAL_YEAR:             'INTERVAL YEAR(2)',
            sqlalch_td.INTERVAL_YEAR_TO_MONTH:    'INTERVAL YEAR(2) TO MONTH',
            sqlalch_td.INTERVAL_MONTH:            'INTERVAL MONTH(2)',
            sqlalch_td.INTERVAL_DAY:              'INTERVAL DAY(2)',
            sqlalch_td.INTERVAL_DAY_TO_HOUR:      'INTERVAL DAY(2) TO HOUR',
            sqlalch_td.INTERVAL_DAY_TO_MINUTE:    'INTERVAL DAY(2) TO MINUTE',
            sqlalch_td.INTERVAL_DAY_TO_SECOND:    'INTERVAL DAY(2) TO SECOND(6)',
            sqlalch_td.INTERVAL_HOUR:             'INTERVAL HOUR(2)',
            sqlalch_td.INTERVAL_HOUR_TO_MINUTE:   'INTERVAL HOUR(2) TO MINUTE',
            sqlalch_td.INTERVAL_HOUR_TO_SECOND:   'INTERVAL HOUR(2) TO SECOND(6)',
            sqlalch_td.INTERVAL_MINUTE:           'INTERVAL MINUTE(2)',
            sqlalch_td.INTERVAL_MINUTE_TO_SECOND: 'INTERVAL MINUTE(2) TO SECOND(6)',
            sqlalch_td.INTERVAL_SECOND:           'INTERVAL SECOND(2,6)',
            sqlalch_td.PERIOD_DATE:               'PERIOD(DATE)',
            sqlalch_td.PERIOD_TIME:               'PERIOD(TIME(6))',
            sqlalch_td.PERIOD_TIMESTAMP:          'PERIOD(TIMESTAMP(6))'
        }

        parsed_attrs = utils.parse_show_table_col_attrs(
            self.conn.execute(
                'SHOW TABLE table_test_types_sqlalch').fetchone().items()[0][1],
            tuple(col.name for col in cols))

        for col, attr in parsed_attrs.items():
            assert(type_map[col_to_type[col]] in attr)

    def test_types_rawsql_select(self):
        """
        Tests that a selection of SQL types correctly translate to the
        corresponding native Python types through selection.

        This is carried out by creating a test table containing the selection of
        types and then querying data from that table to check that the returned
        cursor_description (types) matches expectation. The test table is
        created by directly executing the appropriate DDL on a Teradata
        SQLAlchemy engine.
        """
        stmt = 'CREATE TABLE table_test_types_rawsql (' +\
               ', '.join(['column_' + str(i) + ' ' + str(type) for
               i, type in enumerate(self.rawsql_types)]) + ')'
        Table('table_test_types_rawsql', self.metadata)
        self.conn.execute(stmt)

        col_to_type = {'column_' + str(i): type for
            i, type in enumerate(self.rawsql_types)}
        type_map    = {
            'BIGINT':       decimal.Decimal,
            'SMALLINT':     decimal.Decimal,
            'BYTEINT':      decimal.Decimal,
            'INTEGER':      decimal.Decimal,
            'DECIMAL':      decimal.Decimal,
            'FLOAT':        decimal.Decimal,
            'NUMBER':       decimal.Decimal,
            'DATE':         datetime.date,
            'TIME':         datetime.time,
            'TIMESTAMP':    datetime.datetime,
            'CHARACTER':    str,
            'VARCHAR(50)':  str,
            'CLOB':         str
        }

        res = self.conn.execute('SELECT * FROM table_test_types_rawsql')
        for col in res._cursor_description():
            assert(type_map[col_to_type[col[0]]] == col[1])

    def test_types_rawsql_reflect(self):
        """
        Tests that a selection of SQL types correctly translate to the
        corresponding SQLAlchemy types through reflection.

        This is carried out by creating a test table containing the selection of
        types and then reflecting the table back and checking that each column
        type is consistent with the types the table was created with. The test
        table is created by directly executing the appropriate DDL on a Teradata
        SQLAlchemy engine.
        """
        stmt = 'CREATE TABLE table_test_types_rawsql (' +\
               ', '.join(['column_' + str(i) + ' ' + str(type) for
               i, type in enumerate(self.rawsql_types)]) + ')'
        Table('table_test_types_rawsql', self.metadata)
        self.conn.execute(stmt)

        col_to_type = {'column_' + str(i): type for
            i, type in enumerate(self.rawsql_types)}
        type_map    = {
            'BIGINT':       sqltypes.BIGINT,
            'SMALLINT':     sqltypes.SMALLINT,
            'INTEGER':      sqltypes.INTEGER,
            'FLOAT':        sqltypes.FLOAT,
            'DATE':         sqltypes.DATE,
            'BYTEINT':      sqlalch_td.BYTEINT,
            'DECIMAL':      sqlalch_td.DECIMAL,
            'TIME':         sqlalch_td.TIME,
            'TIMESTAMP':    sqlalch_td.TIMESTAMP,
            'CHARACTER':    sqlalch_td.CHAR,
            'VARCHAR(50)':  sqlalch_td.VARCHAR,
            'CLOB':         sqlalch_td.CLOB,
            'NUMBER':       sqlalch_td.NUMBER
        }

        reflected_cols = self.inspect.get_columns('table_test_types_rawsql')
        for col in reflected_cols:
            assert(type_map[col_to_type[col['name']]] == type(col['type']))

    def test_types_rawsql_show(self):
        """
        Tests that a selection of SQL types correctly translate to the
        corresponding database types through show table.

        This is carried out by creating a test table containing the selection of
        types and then doing a SHOW TABLE query on the created table. The
        returned table string is then parsed to extract the attributes of each
        column and then checked against the expected types. The test table is
        created by directly executing the appropriate DDL on a Teradata
        SQLAlchemy engine.
        """
        stmt = 'CREATE TABLE table_test_types_rawsql (' + \
               ', '.join(['column_' + str(i) + ' ' + str(type) for
               i, type in enumerate(self.rawsql_types)]) + ')'
        Table('table_test_types_rawsql', self.metadata)
        self.conn.execute(stmt)

        col_to_type = {'column_' + str(i): type for
            i, type in enumerate(self.rawsql_types)}
        type_map    = {
            'BIGINT':       'BIGINT',
            'SMALLINT':     'SMALLINT',
            'BYTEINT':      'BYTEINT',
            'INTEGER':      'INTEGER',
            'DECIMAL':      'DECIMAL',
            'FLOAT':        'FLOAT',
            'NUMBER':       'NUMBER',
            'DATE':         'DATE',
            'TIME':         'TIME',
            'TIMESTAMP':    'TIMESTAMP',
            'CHARACTER':    'CHAR',
            'VARCHAR(50)':  'VARCHAR',
            'CLOB':         'CLOB'
        }

        parsed_attrs = utils.parse_show_table_col_attrs(
            self.conn.execute(
                'SHOW TABLE table_test_types_rawsql').fetchone().items()[0][1],
            tuple('column_' + str(i) for i in range(len(self.rawsql_types))))

        for col, attr in parsed_attrs.items():
            assert(type_map[col_to_type[col]] in attr)


class TestTypesDDLDetailed(testing.fixtures.TestBase):

    """
    This class conducts more detailed testing over each category of types
    currently supported by the Teradata dialect:

        - Binary:     BYTE, VARBYTE, BLOB

        - Numeric:    BIGINT, BYTEINT, DATE, DECIMAL, FLOAT, INTEGER, NUMBER,
                      SMALLINT

        - Datetime:   DATE, TIME, TIMESTAMP

        - Interval:   INTERVAL YEAR, INTERVAL YEAR TO MONTH, INTERVAL MONTH,
                      INTERVAL DAY, INTERVAL DAY TO HOUR, INTERVAL DAY TO MINUTE,
                      INTERVAL DAY TO SECOND, INTERVAL HOUR, INTERVAL HOUR TO
                      MINUTE, INTERVAL HOUR TO SECOND, INTERVAL MINUTE, INTERVAL
                      MINUTE TO SECOND, INTERVAL SECOND

        - Character:  CHAR, VARCHAR, CLOB

        - Period:     PERIOD(DATE), PERIOD(TIME), PERIOD(TIMESTAMP)

    Each category corresponds to a single test method which tests that both
    the types are correctly interpreted, and that the type attributes at
    instantiation are correctly communicated to the DB and preserved after
    reflection. For types whose bind/result processing may be open to further
    changes, insertion into columns with those types are also tested.
    """

    def setup(self):
        self.conn     = testing.db.connect()
        self.engine   = self.conn.engine
        self.metadata = MetaData(bind=self.engine)
        self.inspect  = reflection.Inspector.from_engine(self.engine)

    def tearDown(self):
        self.metadata.drop_all(self.engine)
        self.conn.close()

    def test_types_binary(self):
        """
        Tests the correctness of the Binary type(s) (BYTE, VARBYTE, BLOB)
        implementation.

        This is done by first creating a test table with columns corresponding
        to each of the available Binary types (each with a certain attribute
        configuration). Subsequently, the following tests are carried out:

        (1) Inspect the column types by reflection to see that each column is
            of the expected type and possesses the expected attribute values.

        (2) Insert some byte data into each of the columns. This data is then
            queried back and checked against the string that was previously
            inserted into the row. The data received back is expected to have
            been truncated if the byte length is longer than that specified by
            the column.
        """

        col_types = {
            'column_0': sqlalch_td.BYTE(length=10),
            'column_1': sqlalch_td.VARBYTE(length=100),
            'column_2': sqlalch_td.BLOB(length=1000)
        }

        # Create the test table with the above Binary types
        cols  = [Column(name, type) for name, type in col_types.items()]
        table = Table('table_test_types_binary', self.metadata, *cols)
        self.metadata.create_all(checkfirst=False)

        # test that each reflected column type has all the attribute values it
        # was instantiated with
        reflected_cols = self.inspect.get_columns('table_test_types_binary')
        for col in reflected_cols:
            assert(type(col['type']) == type(col_types[col['name']]))
            assert(str(col['type'].__dict__) ==
                str(col_types[col['name']].__dict__))

        text = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed '   \
               'do eiusmod tempor incididunt ut labore et dolore magna aliqua. ' \
               'Ut enim ad minim veniam, quis nostrud exercitation ullamco '     \
               'laboris nisi ut aliquip ex ea commodo consequat. Duis aute '     \
               'irure dolor in reprehenderit in voluptate velit esse cillum '    \
               'dolore eu fugiat nulla pariatur. Excepteur sint occaecat '       \
               'cupidatat non proident, sunt in culpa qui officia deserunt '     \
               'mollit anim id est laborum.'

        text = str.encode(text)
        # Insert the text above, encoded as a bytearray, into each column
        self.conn.execute(table.insert(),
            {'column_0': text,
             'column_1': text,
             'column_2': text})

        res = self.engine.execute(table.select())

        res = tuple(str(r) for r in res.fetchone())
        assert(res[0] ==
            "b'Lorem ipsu'")
        assert(res[1] ==
            "b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do "
            "eiusmod tempor incididunt ut labore '")
        assert(res[2] ==
            "b'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do "
            "eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim "
            "ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut "
            "aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit "
            "in voluptate velit esse cillum dolore eu fugiat nulla pariatur. "
            "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui "
            "officia deserunt mollit anim id est laborum.'")

    def test_types_numeric(self):
        """
        Tests the correctness of the Numeric type(s) (INTEGER, BIGINT, SMALLINT,
        BYTEINT, FLOAT, DECIMAL, NUMBER) implementation.

        This is done by creating a table where each column is a Numeric type
        (for NUMBER and DECIMAL each has some precision and scale value). Then,
        each of the columns are inspected via reflection to ensure that they are
        all of the expected type and have the attribute values they were
        instantiated with.
        """

        col_types = {
            'column_0': sqlalch_td.INTEGER(),
            'column_1': sqlalch_td.BIGINT(),
            'column_2': sqlalch_td.SMALLINT(),
            'column_3': sqlalch_td.BYTEINT(),
            'column_5': sqlalch_td.FLOAT(),
            'column_4': sqlalch_td.DECIMAL(precision=10, scale=5),
            'column_6': sqlalch_td.NUMBER(precision=38, scale=20),
            # 'column_7': sqlalch_td.NUMBER() TODO issue with -128 prec and scale
        }

        # Create the test table with the above Numeric types
        cols  = [Column(name, type) for name, type in col_types.items()]
        table = Table('table_test_types_numeric', self.metadata, *cols)
        self.metadata.create_all(checkfirst=False)

        # test that each reflected column type has all the attribute values it
        # was instantiated with
        reflected_cols = self.inspect.get_columns('table_test_types_numeric')
        for col in reflected_cols:
            assert(type(col['type']) == type(col_types[col['name']]))
            assert(str(col['type'].__dict__) ==
                str(col_types[col['name']].__dict__))

    def test_types_datetime(self):
        """
        Tests the correctness of the Datetime type(s) (DATE, TIME, TIMESTAMP)
        implementation.

        This is done by creating a table where each column is a Datetime type
        (for TIME and TIMESTAMP each has some precision and timezone value).
        Then, each of the columns are inspected via reflection to ensure that
        they are all of the expected type and have the attribute values they
        were instantiated with.
        """

        col_types = {
            'column_0': sqlalch_td.DATE(),
            'column_1': sqlalch_td.TIME(precision=5, timezone=True),
            'column_2': sqlalch_td.TIMESTAMP(precision=4, timezone=True)
        }

        # Create the test table with the above Datetime types
        cols  = [Column(name, type) for name, type in col_types.items()]
        table = Table('table_test_types_standard', self.metadata, *cols)
        self.metadata.create_all(checkfirst=False)

        # test that each reflected column type has all the attribute values it
        # was instantiated with
        reflected_cols = self.inspect.get_columns('table_test_types_standard')
        for col in reflected_cols:
            assert(type(col['type']) == type(col_types[col['name']]))
            assert(str(col['type'].__dict__) ==
                str(col_types[col['name']].__dict__))

    def test_types_interval(self):
        """
        Tests the correctness of the Teradata Interval type(s) implementation.

        This is done by first creating a test table with columns corresponding
        to each of the available Interval types (each with a certain attribute
        configuration). Subsequently, the following tests are carried out:

        (1) Inspect the column types by reflection to see that each column is
            of the expected type and possesses the expected attribute values.

        (2) Insert some data into each of the columns in the form of strings,
            timedelta, and Teradata Interval objects. This data is then queried
            back and checked against its expected string representation.
        """

        col_types = {
            'column_0':  sqlalch_td.INTERVAL_YEAR(precision=3),
            'column_1':  sqlalch_td.INTERVAL_YEAR_TO_MONTH(precision=3),
            'column_2':  sqlalch_td.INTERVAL_MONTH(precision=3),
            'column_3':  sqlalch_td.INTERVAL_DAY(precision=3),
            'column_4':  sqlalch_td.INTERVAL_DAY_TO_HOUR(precision=3),
            'column_5':  sqlalch_td.INTERVAL_DAY_TO_MINUTE(precision=3),
            'column_6':  sqlalch_td.INTERVAL_DAY_TO_SECOND(precision=3, frac_precision=5),
            'column_7':  sqlalch_td.INTERVAL_HOUR(precision=3),
            'column_8':  sqlalch_td.INTERVAL_HOUR_TO_MINUTE(precision=3),
            'column_9':  sqlalch_td.INTERVAL_HOUR_TO_SECOND(precision=3, frac_precision=5),
            'column_10': sqlalch_td.INTERVAL_MINUTE(precision=3),
            'column_11': sqlalch_td.INTERVAL_MINUTE_TO_SECOND(precision=3, frac_precision=5),
            'column_12': sqlalch_td.INTERVAL_SECOND(precision=3, frac_precision=5)
        }

        # Create the test table with the above Interval types
        # Note the copy() is here because otherwise after table creation, the type
        # instances defined in the col_types dict will each get populated with a
        # "dispatcher" field which breaks the equality check further along in
        # this test
        cols  = [Column(name, type.copy()) for name, type in col_types.items()]
        table = Table('table_test_types_interval', self.metadata, *cols)
        self.metadata.create_all(checkfirst=False)

        # Test that each reflected column type has all the attribute values it
        # was instantiated with
        reflected_cols = self.inspect.get_columns('table_test_types_interval')
        for col in reflected_cols:
            assert(type(col['type']) == type(col_types[col['name']]))
            assert(str(col['type'].__dict__) ==
                str(col_types[col['name']].__dict__))

        # Insert three rows of data (with different types) into the test table
        self.conn.execute(table.insert(),
            {'column_0':  None,
             'column_1':  None,
             'column_2':  None,
             'column_3':  datetime.timedelta(days=365),
             'column_4':  datetime.timedelta(days=365, hours=24),
             'column_5':  datetime.timedelta(days=365, minutes=60),
             'column_6':  datetime.timedelta(days=365, seconds=60.123),
             'column_7':  datetime.timedelta(hours=24),
             'column_8':  datetime.timedelta(hours=24, minutes=60),
             'column_9':  datetime.timedelta(hours=24, seconds=60.123),
             'column_10': datetime.timedelta(minutes=60),
             'column_11': datetime.timedelta(minutes=60, seconds=60.123),
             'column_12': datetime.timedelta(seconds=60.12345)},
            {'column_0':  '10',
             'column_1':  '10-10',
             'column_2':  '10',
             'column_3':  '10',
             'column_4':  '10 10',
             'column_5':  '10 00:10',
             'column_6':  '10 00:00:10.10',
             'column_7':  '10',
             'column_8':  '10:10',
             'column_9':  '10:00:10.10',
             'column_10': '10',
             'column_11': '10:10.10',
             'column_12': '10.10'},
            {'column_0':  td_dtypes.Interval(years=20),
             'column_1':  td_dtypes.Interval(years=20, months=20),
             'column_2':  td_dtypes.Interval(months=20),
             'column_3':  td_dtypes.Interval(days=20),
             'column_4':  td_dtypes.Interval(days=20, hours=20),
             'column_5':  td_dtypes.Interval(days=20, minutes=20),
             'column_6':  td_dtypes.Interval(days=20, seconds=20.20),
             'column_7':  td_dtypes.Interval(hours=20),
             'column_8':  td_dtypes.Interval(hours=20, minutes=20),
             'column_9':  td_dtypes.Interval(hours=20, seconds=20.20),
             'column_10': td_dtypes.Interval(minutes=20),
             'column_11': td_dtypes.Interval(minutes=20, seconds=20.20),
             'column_12': td_dtypes.Interval(seconds=20.20)})
        res = self.conn.execute(table.select().order_by(table.c.column_0))

        # Test that insertion by strings, timedelta, and Interval objects are
        # correctly handled by checking that the string representation of each
        # row that is queried back is as expected
        assert(str([str(c) for c in res.fetchone()]) ==
            "['None', 'None', 'None', '365', '366 00', '365 01:00', "
            "'365 00:01:00.123', '24', '25:00', '24:01:00.123', '60', "
            "'61:00.123', '60.12345']")
        assert(str([str(c) for c in res.fetchone()]) ==
            "['10', '10-10', '10', '10', '10 10', '10 00:10', '10 00:00:10.1', "
            "'10', '10:10', '10:00:10.1', '10', '10:10.1', '10.1']")
        assert(str([str(c) for c in res.fetchone()]) ==
            "['20', '21-08', '20', '20', '20 20', '20 00:20', '20 00:00:20.2', "
            "'20', '20:20', '20:00:20.2', '20', '20:20.2', '20.2']")

    def test_types_character(self):
        """
        Tests the correctness of the Character type(s) (CHAR, VARCHAR)
        implementation.

        This is done by creating a table where each column is a Character type
        (with a certain combination of length and charset values). Then, each
        of the columns are inspected via reflection to ensure that they are
        all of the expected type and have the attribute values they were
        instantiated with.
        """

        col_types = {
            'column_0':  sqlalch_td.CHAR(length=1, charset='LATIN'),
            'column_1':  sqlalch_td.CHAR(length=2, charset='UNICODE'),
            'column_2':  sqlalch_td.CHAR(length=3, charset='KANJISJIS'),
            'column_3':  sqlalch_td.CHAR(length=4, charset='GRAPHIC'),
            'column_4':  sqlalch_td.VARCHAR(length=1, charset='LATIN'),
            'column_5':  sqlalch_td.VARCHAR(length=2, charset='UNICODE'),
            'column_6':  sqlalch_td.VARCHAR(length=3, charset='KANJISJIS'),
            'column_7':  sqlalch_td.VARCHAR(length=4, charset='GRAPHIC'),
            'column_8':  sqlalch_td.VARCHAR(charset='LATIN'),
            'column_9':  sqlalch_td.VARCHAR(charset='UNICODE'),
            'column_10': sqlalch_td.VARCHAR(charset='KANJISJIS'),
            'column_11': sqlalch_td.VARCHAR(charset='GRAPHIC')
        }

        # Create the test table with the above Character types
        cols  = [Column(name, type) for name, type in col_types.items()]
        table = Table('table_test_types_character', self.metadata, *cols)
        self.metadata.create_all(checkfirst=False)

        # The types expected to be reflected back, this is here mostly to check
        # that the LONG VARCHAR types correctly instantiate to its default
        # length w.r.t. each charset
        exp_types = {
            'column_0':  sqlalch_td.CHAR(length=1, charset='LATIN'),
            'column_1':  sqlalch_td.CHAR(length=2, charset='UNICODE'),
            'column_2':  sqlalch_td.CHAR(length=3, charset='KANJISJIS'),
            'column_3':  sqlalch_td.CHAR(length=4, charset='GRAPHIC'),
            'column_4':  sqlalch_td.VARCHAR(length=1, charset='LATIN'),
            'column_5':  sqlalch_td.VARCHAR(length=2, charset='UNICODE'),
            'column_6':  sqlalch_td.VARCHAR(length=3, charset='KANJISJIS'),
            'column_7':  sqlalch_td.VARCHAR(length=4, charset='GRAPHIC'),
            'column_8':  sqlalch_td.VARCHAR(length=64000, charset='LATIN'),
            'column_9':  sqlalch_td.VARCHAR(length=32000, charset='UNICODE'),
            'column_10': sqlalch_td.VARCHAR(length=32000, charset='KANJISJIS'),
            'column_11': sqlalch_td.VARCHAR(length=32000, charset='GRAPHIC')
        }

        # test that each reflected column type has all the attribute values it
        # was instantiated with
        reflected_cols = self.inspect.get_columns('table_test_types_character')
        for col in reflected_cols:
            assert(type(col['type']) == type(col_types[col['name']]))
            assert(str(col['type'].__dict__) ==
                str(exp_types[col['name']].__dict__))

    def test_types_period(self):
        """
        Tests the correctness of the Teradata Period type(s) implementation.

        This is done by first creating a test table with columns corresponding
        to each of the available Period types (each with a certain attribute
        configuration). Subsequently, the following tests are carried out:

        (1) Inspect the column types by reflection to see that each column is
            of the expected type and possesses the expected attribute values.

        (2) Insert some data into each of the columns in the form of both
            strings and teradata Period objects. This data is then queried
            back and checked against its expected string representation.
        """

        col_types = {
            'column_0': sqltypes.INTEGER(),
            'column_1': sqlalch_td.PERIOD_DATE(
                            format='yyyy-mm-dd'),
            'column_2': sqlalch_td.PERIOD_TIMESTAMP(
                            frac_precision=5,
                            format='YYYY-MM-DDBHH:MI:SS.S(5)'),
            'column_3': sqlalch_td.PERIOD_TIME(
                            frac_precision=4,
                            format='HH:MI:SS.S(4)'),
            'column_4': sqlalch_td.PERIOD_TIMESTAMP(
                            frac_precision=6,
                            timezone=True,
                            format='YYYY-MM-DDBHH:MI:SS.S(6)Z'),
            'column_5': sqlalch_td.PERIOD_TIME(
                            frac_precision=6,
                            timezone=True,
                            format='HH:MI:SS.S(6)Z')
        }

        # Create the test table with the above Period types
        cols  = [Column(name, type) for name, type in col_types.items()]
        table = Table('table_test_types_period', self.metadata, *cols)
        self.metadata.create_all(checkfirst=False)

        # Test that each reflected column type has all the attribute values it
        # was instantiated with
        reflected_cols = self.inspect.get_columns('table_test_types_period')
        for col in reflected_cols:
            assert(type(col['type']) == type(col_types[col['name']]))
            assert(str(col['type'].__dict__) ==
                str(col_types[col['name']].__dict__))

        # Insert two rows of data (with different types) into the test table
        self.conn.execute(table.insert(),
            {'column_0': 0,
             'column_1': "('2010-03-01', "
                         "'2010-08-01')",
             'column_2': "('2010-04-21 08:00:00.12345', "
                         "'2010-04-21 17:00:00.12345')",
             'column_3': "('08:00:00.1234', "
                         "'17:00:00.1234')",
             'column_4': "('2010-04-21 08:00:00.000000+08:00', "
                         "'2010-04-21 17:00:00.000000-08:00')",
             'column_5': "('08:00:00.000000+08:00', "
                         "'17:00:00.000000-08:00')"},
            {'column_0': 1,
             'column_1': td_dtypes.Period(
                datetime.date(2007, 5, 12),
                datetime.date(2018, 7, 13)),
             'column_2': td_dtypes.Period(
                datetime.datetime(2007, 5, 12, 5, 12, 32),
                datetime.datetime(2018, 7, 13, 5, 32, 54)),
             'column_3': td_dtypes.Period(
                datetime.time(5, 12, 32),
                datetime.time(5, 32, 54)),
             'column_4': td_dtypes.Period(
                datetime.datetime(2007, 5, 12, 5, 12, 32),
                datetime.datetime(2018, 7, 13, 5, 32, 54)),
             'column_5': td_dtypes.Period(
                datetime.time(5, 12, 32),
                datetime.time(5, 32, 54))})
        res = self.conn.execute(table.select().order_by(table.c.column_0))

        # Test that both insertion by strings and Period objects are correctly
        # handled by checking that the string representation of each row that
        # is queried back is as expected
        assert(str([str(c) for c in res.fetchone()]) ==
            "['0', "
            "\"('2010-03-01', '2010-08-01')\", "
            "\"('2010-04-21 08:00:00.123450', '2010-04-21 17:00:00.123450')\", "
            "\"('08:00:00.123400', '17:00:00.123400')\", "
            "\"('2010-04-21 08:00:00+08:00', '2010-04-21 17:00:00-08:00')\", "
            "\"('08:00:00+08:00', '17:00:00-08:00')\"]")
        assert(str([str(c) for c in res.fetchone()]) ==
            "['1', "
            "\"('2007-05-12', '2018-07-13')\", "
            "\"('2007-05-12 05:12:32', '2018-07-13 05:32:54')\", "
            "\"('05:12:32', '05:32:54')\", "
            "\"('2007-05-12 05:12:32+00:00', '2018-07-13 05:32:54+00:00')\", "
            "\"('05:12:32+00:00', '05:32:54+00:00')\"]")


def test_decorator(test_fn):

    def test_wrapper_fn(self):
        test_fn(self)
        self.metadata.create_all(checkfirst=False)
        self._test_tables_created(self.metadata, self.engine)

    test_wrapper_fn.__name__ = test_fn.__name__
    return test_wrapper_fn


class TestCreateIndexDDL(testing.fixtures.TestBase):

    def setup(self):
        self.conn     = testing.db.connect()
        self.engine   = self.conn.engine
        self.metadata = MetaData(bind=self.engine)

    def tearDown(self):
        self.metadata.drop_all(self.engine)
        self.conn.invalidate()
        self.conn.close()

    def _test_tables_created(self, metadata, engine):
        """
        Asserts that all the tables within the passed in metadata exists on the
        database of the specified engine and are the only tables on that
        database.

        Args:
            metadata: A MetaData instance containing the tables to check for.
            engine:   An Engine instance associated with a dialect and database
                      for which the tables in the metadata are to be checked for.

        Raises:
            AssertionError: Raised when the set of tables contained in the
                            metadata is not equal to the set of tables on
                            the engine's associated database.
        """
        assert(
            set([tablename for tablename, _ in metadata.tables.items()]) ==
            set(engine.table_names()))

    # TODO Add a more thorough test to check that indexes are actually being
    #      created on the database
    # def _test_indexes_created(self, metadata, engine):

    @test_decorator
    def test_create_index_inline(self):
        """
        Tests creating tables with an inline column index.
        """
        my_table = Table('t_index_inline', self.metadata,
                        Column('c1', NUMERIC),
                        Column('c2', DECIMAL, index=True))

    @test_decorator
    def test_create_index_construct(self):
        """
        Tests creating tables with the schema index construct.
        """
        my_table = Table('t_index_construct', self.metadata,
                        Column('c1', NUMERIC),
                        Column('c2', DECIMAL))
        Index('i', my_table.c.c2)

    @test_decorator
    def test_create_multiple_indexes(self):
        """
        Tests creating tables with multiple indexes.
        """
        my_table = Table('t_multiple_indexes', self.metadata,
                        Column('c1', NUMERIC),
                        Column('c2', DECIMAL))
        Index('i', my_table.c.c1, my_table.c.c2)

    @test_decorator
    def test_create_index_unique(self):
        """
        Tests creating tables with a unique index.
        """
        my_table = Table('t_index_unique', self.metadata,
                        Column('c', NUMERIC, index=True, unique=True))

    @test_decorator
    def test_create_multiple_unique_indexes(self):
        """
        Tests creating tables with multiple unique indexes.
        """
        my_table = Table('t_multiple_unique_indexes', self.metadata,
                        Column('c1', NUMERIC),
                        Column('c2', DECIMAL))
        Index('i', my_table.c.c1, my_table.c.c2, unique=True)

    @test_decorator
    def test_create_index_noname(self):
        """
        Tests creating tables with an index without an index name.
        """
        my_table = Table('t_index_noname', self.metadata,
                        Column('c1', NUMERIC),
                        Column('c2', DECIMAL))
        Index(None, my_table.c.c2)


class TestCreateSuffixDDL(testing.fixtures.TestBase):

    def setup(self):
        self.conn     = testing.db.connect()
        self.engine   = self.conn.engine
        self.metadata = MetaData(bind=self.engine)

    def tearDown(self):
        self.metadata.drop_all(self.engine)
        self.conn.invalidate()
        self.conn.close()

    def _generate_table_name(self, base, opt):
        """
        Generates a unique table name for each base and option argument
        with the following form:

            't_base_opt1_op2 ... _optn'

        Args:
            base: The base name of the table.
            opt:  The option passed to the table generation process. This is
                  expected to either be a primitive or a tuple of primitives.

        Returns:
            A unique table name of a particular form based on the
            specified arguments.
        """
        return 't_' + base + '_' +\
            ('_'.join([str(arg).lower() for
                arg in opt if arg is not None])
            if isinstance(opt, tuple)
            else str(opt).lower())

    def _create_tables_with_suffix_opts(self, suffix, opts, metadata):
        """
        Create tables each with a particular TDCreateTableSuffix over various
        options of the suffix (and bind each table to the passed in metadata).

        Args:
            suffix:   The TDCreateTableSuffix (function) to create the
                      tables with.
            opts:     The various options of the suffix to create the
                      tables with. This is expected to either be a list of
                      tuples or list of primitives.
            metadata: The metadata to bind all created tables to.
        """
        for opt in opts:
            Table(
                self._generate_table_name(suffix.__name__, opt),
                metadata,
                Column('c', Integer),
                teradata_suffixes=suffix(*opt)
                    if isinstance(opt, tuple)
                    else suffix(opt))

    def _test_tables_created(self, metadata, engine):
        """
        Asserts that all the tables within the passed in metadata exists on the
        database of the specified engine and are the only tables on that
        database.

        Args:
            metadata: A MetaData instance containing the tables to check for.
            engine:   An Engine instance associated with a dialect and database
                      for which the tables in the metadata are to be checked for.

        Raises:
            AssertionError: Raised when the set of tables contained in the
                            metadata is not equal to the set of tables on
                            the engine's associated database.
        """
        assert(
            set([tablename for tablename, _ in metadata.tables.items()]) ==
            set(engine.table_names()))

    @test_decorator
    def test_create_suffix_fallback(self):
        """
        Tests creating tables with the fallback suffix and the following
        option(s):

            enabled = True, False
        """
        opts = (True, False)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().fallback, opts, self.metadata)

    @test_decorator
    def test_create_suffix_log(self):
        """
        Tests creating tables with the log suffix and the following
        option(s):

            enabled = True
        """
        opts = (True,)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().log, opts, self.metadata)

    def test_create_suffix_nolog_err(self):
        """
        Tests for specific error(s) when creating tables with the log suffix
        and the following option(s):

            enabled = False
        """
        with pytest.raises(Exception) as exc_info:
            opts = (False,)
            self._create_tables_with_suffix_opts(
                TDCreateTableSuffix().log, opts, self.metadata)

            self.metadata.create_all(checkfirst=False)

        assert('NO LOG keywords not allowed for permanent table' in
            str(exc_info.value))
        assert(not self.engine.has_table('t_log_false'))

    # TODO Add test(s) for journaling (currently unable to create a new permanent
    #      journal for the database through MODIFY, can through CREATE but
    #      creating a new database during testing is not ideal)
    # def test_create_suffix_journal(self):

    @test_decorator
    def test_create_suffix_checksum(self):
        """
        Tests creating tables with the checksum suffix and the following
        option(s):

            integrity_checking = 'default', 'off', 'on'
        """
        opts = ('default', 'off', 'on')
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().checksum, opts, self.metadata)

    @test_decorator
    def test_create_suffix_freespace(self):
        """
        Tests creating tables with the freespace suffix and the following
        option(s):

            percentage = 0, 75, 40
        """
        opts = (0, 75, 40)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().freespace, opts, self.metadata)

    def test_create_suffix_freespace_err(self):
        """
        Tests for specific error(s) when creating tables with the freespace suffix
        and the following option(s):

            percentage = 100
        """
        with pytest.raises(Exception) as exc_info:
            opts = (100,)
            self._create_tables_with_suffix_opts(
                TDCreateTableSuffix().freespace, opts, self.metadata)

            self.metadata.create_all(checkfirst=False)

        assert('The specified FREESPACE value is not between 0 and 75 percent' in
            str(exc_info.value))
        assert(not self.engine.has_table('t_freespace_100'))

    @test_decorator
    def test_create_suffix_mergeblockratio(self):
        """
        Tests creating tables with the mergeblockratio suffix and the following
        option(s):

            integer = None, 0, 100, 50
        """
        opts = (None, 0, 100, 50)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().mergeblockratio, opts, self.metadata)

        Table('t_no_mergeblockratio', self.metadata,
            Column('c', Integer),
            teradata_suffixes=TDCreateTableSuffix().no_mergeblockratio())

    def test_create_suffix_mergeblockratio_err(self):
        """
        Tests for specific error(s) when creating tables with the mergeblockratio
        suffix and the following option(s):

            integer = 101
        """
        with pytest.raises(Exception) as exc_info:
            opts = (101,)
            self._create_tables_with_suffix_opts(
                TDCreateTableSuffix().mergeblockratio, opts, self.metadata)

            self.metadata.create_all(checkfirst=False)

        assert('The specified MERGEBLOCKRATIO value is invalid' in
            str(exc_info.value))
        assert(not self.engine.has_table('t_mergeblockratio_101'))

    @test_decorator
    def test_create_suffix_datablocksize(self):
        """
        Tests creating tables with the datablocksize suffix and the following
        option(s):

            (datablocksize) data_block_size = None, 21248
            min_datablocksize
            max_datablocksize
        """
        opts = (None, 21248)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().datablocksize, opts, self.metadata)

        Table('t_datablocksize_min', self.metadata,
            Column('c', Integer),
            teradata_suffixes=TDCreateTableSuffix().min_datablocksize())

        Table('t_datablocksize_max', self.metadata,
            Column('c', Integer),
            teradata_suffixes=TDCreateTableSuffix().max_datablocksize())

    def test_create_suffix_datablocksize_err(self):
        """
        Tests for specific error(s) when creating tables with the datablocksize
        suffix and the following option(s):

            data_block_size = 1024
        """
        with pytest.raises(Exception) as exc_info:
            opts = (1024,)
            self._create_tables_with_suffix_opts(
                TDCreateTableSuffix().datablocksize, opts, self.metadata)

            self.metadata.create_all(checkfirst=False)

        assert('The specified DATABLOCKSIZE value must be within ' \
            'the range of 21248 and 1048319 bytes' in
            str(exc_info.value))
        assert(not self.engine.has_table('t_datablocksize_1024'))

    @test_decorator
    def test_create_suffix_blockcompression(self):
        """
        Tests creating tables with the blockcompression suffix and the following
        option(s):

            opt = 'default', 'autotemp', 'manual', 'never'
        """
        opts = ('default', 'autotemp', 'manual', 'never')
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().blockcompression, opts, self.metadata)

    @test_decorator
    def test_create_suffix_no_isolated_loading(self):
        """
        Tests creating tables with the no_isolated_loading suffix and the following
        option(s):

            concurrent = False, True
        """
        opts = (False, True)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().with_no_isolated_loading, opts, self.metadata)

    @test_decorator
    def test_create_suffix_isolated_loading(self):
        """
        Tests creating tables with the isolated_loading suffix and the following
        (combination(s) of) option(s):

            concurrent = False, True
            opts       = 'all', 'insert', 'none', None
        """
        concurrent_opts = (True, False)
        for_opts        = ('all', 'insert', 'none', None)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().with_isolated_loading,
            itertools.product(concurrent_opts, for_opts), self.metadata)


class TestCreatePostCreateDDL(testing.fixtures.TestBase):

    def setup(self):
        self.conn     = testing.db.connect()
        self.engine   = self.conn.engine
        self.metadata = MetaData(bind=self.engine)

    def tearDown(self):
        self.metadata.drop_all(self.engine)
        self.conn.invalidate()
        self.conn.close()

    def _generate_table_name(self, base, opt):
        """
        Generates a unique table name for each base and option argument
        with the following form:

            't_base_opt1_op2 ... _optn'

        Args:
            base: The base name of the table.
            opt:  The option passed to the table generation process. This is
                  expected to either be a primitive or a tuple of either
                  primitives, tuples, lists, or dicts.

        Returns:
            A unique table name of a particular form based on the
            specified arguments.
        """
        return 't_' + base + '_' +\
            ('_'.join(
                [('_'.join([str(e).lower() for e in arg])
                if isinstance(arg, (tuple, list, dict))
                else str(arg).lower()) for
                    arg in opt if arg is not None])
            if isinstance(opt, tuple)
            else str(opt).lower())

    def _create_tables_with_post_opts(self, post, opts, metadata):
        """
        Create tables each with a particular TDCreateTablePost over various
        options of the post_create (and bind each table to the passed in metadata).

        Args:
            post:     The TDCreateTablePost (function) to create the
                      tables with.
            opts:     The various options of the post_create to create the
                      tables with. This is expected to either be a list of
                      tuples or list of primitives.
            metadata: The metadata to bind all created tables to.
        """
        for opt in opts:
            Table(
                self._generate_table_name(post.__name__, opt),
                metadata,
                Column('c1', NUMERIC),
                Column('c2', DECIMAL),
                Column('c3', VARCHAR),
                Column('c4', CHAR),
                Column('c5', CLOB),
                teradata_post_create=post(*opt)
                    if isinstance(opt, tuple)
                    else post(opt))

    def _test_tables_created(self, metadata, engine):
        """
        Asserts that all the tables within the passed in metadata exists on the
        database of the specified engine and are the only tables on that
        database.

        Args:
            metadata: A MetaData instance containing the tables to check for.
            engine:   An Engine instance associated with a dialect and database
                      for which the tables in the metadata are to be checked for.

        Raises:
            AssertionError: Raised when the set of tables contained in the
                            metadata is not equal to the set of tables on
                            the engine's associated database.
        """
        assert(
            set([tablename for tablename, _ in metadata.tables.items()]) ==
            set(engine.table_names()))

    @test_decorator
    def test_create_post_no_primary_index(self):
        """
        Tests creating tables with the no_primary_index post_create.
        """
        Table('t_no_primary_index', self.metadata,
            Column('c', Integer),
            teradata_post_create=TDCreateTablePost().no_primary_index())

    @test_decorator
    def test_create_post_primary_index(self):
        """
        Tests creating tables with the primary_index post_create and the following
        (combination(s) of) option(s):

            name   = None, 'primary_index_name'
            unique = False, True
            cols   = ['c1'], ['c1', 'c3']
        """
        name_opts   = (None, 'primary_index_name')
        unique_opts = (False, True)
        cols_opts   = (['c1'], ['c1', 'c3'])
        self._create_tables_with_post_opts(
            TDCreateTablePost().primary_index,
            itertools.product(name_opts, unique_opts, cols_opts), self.metadata)

    # TODO Add test(s) for primary_amp (which requires partition_by)
    # def test_create_post_primary_amp(self):
    #     name_opts = (None, "primary_amp_name")
    #     cols_opts = (['c1'], ['c1', 'c3'])
    #     self._create_tables_with_post_opts(
    #         TDCreateTablePost().primary_amp,
    #         itertools.product(name_opts, cols_opts), self.metadata)
    #
    #     self.metadata.create_all(checkfirst=False)
    #     self._test_tables_created(self.metadata, self.engine)

    # TODO Add test(s) for partition_by_col (which is currently not working due
    #      to issues with "column partitioning not supported by system")
    # def test_create_post_partition_by(self):
    #     all_but_opts = (False, True)
    #     cols_opts    = (
    #         {'c1': True},
    #         {'c1': True, 'c3': False, 'c5': None})
    #     rows_opts    = (
    #         {'d1': True},
    #         {'d1': True, 'd3': False, 'd5': None})
    #     const_opts   = (None, 1)
    #
    #     self._create_tables_with_post_opts(
    #         TDCreateTablePost().no_primary_index().partition_by_col,
    #         itertools.product(all_but_opts, cols_opts, rows_opts, const_opts), self.metadata)
    #
    #     self.metadata.create_all(checkfirst=False)
    #     self._test_tables_created(self.metadata, self.engine)

    @test_decorator
    def test_create_post_unique_index(self):
        """
        Tests creating tables with the unique_index post_create and the following
        (combination(s) of) option(s):

            name = None, 'unique_index_name'
            cols = ['c2'], ['c2', 'c3']
        """
        name_opts = (None, 'unique_index_name')
        cols_opts = (['c2'], ['c2', 'c3'])
        self._create_tables_with_post_opts(
            TDCreateTablePost().unique_index,
            itertools.product(name_opts, cols_opts), self.metadata)

    def test_create_post_unique_index_err(self):
        """
        Tests for specific error(s) when creating tables with the unique_index
        post_create and the following (combination(s) of) option(s):

            name = None
            cols = ['c1']
        """
        with pytest.raises(Exception) as exc_info:
            opts = [(None, ['c1'])]
            self._create_tables_with_post_opts(
                TDCreateTablePost().unique_index, opts, self.metadata)

            self.metadata.create_all(checkfirst=False)

        assert('Two indexes with the same columns' in str(exc_info.value))
        assert(not self.engine.has_table('t_unique_index_c1'))
