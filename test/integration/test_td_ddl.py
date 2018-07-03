from sqlalchemy import *
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import testing
from sqlalchemy.engine import reflection
from sqlalchemy.testing.plugin.pytestplugin import *

import sqlalchemy_teradata as sqlalch_td
import sqlalchemy.sql as sql
import decimal, datetime

import utils

"""
Test DDL Expressions and Dialect Extensions
The tests are based of off SQL Data Definition Language (Release 15.10, Dec '15)
"""

class TestCompileCreateTableDDL(testing.fixtures.TestBase):

    def setup(self):
        self.conn     = testing.db.connect()
        self.engine   = self.conn.engine
        self.metadata = MetaData(bind=self.engine)
        self.inspect  = reflection.Inspector.from_engine(self.engine)

        self.sqlalch_types = sqlalch_td.__all__
        self.rawsql_types  = [
            'CHARACTER', 'VARCHAR(50)', 'CLOB', 'BIGINT', 'SMALLINT',
            'BYTEINT', 'INTEGER', 'DECIMAL', 'FLOAT', 'NUMBER',
            'DATE', 'TIME', 'TIMESTAMP'
        ]

    def tearDown(self):
        self.metadata.drop_all(self.engine)
        self.conn.close()

    def test_types_sqlalch_select(self):
        cols  = [Column('column_' + str(i), type)
            for i, type in enumerate(self.sqlalch_types)]
        table = Table('table_test_types_sqlalch', self.metadata, *cols)
        self.metadata.create_all()

        col_to_type = {col.name: type(col.type) for col in cols}
        type_map    = {
            sqlalch_td.Integer:      decimal.Decimal,
            sqlalch_td.SmallInteger: decimal.Decimal,
            sqlalch_td.BigInteger:   decimal.Decimal,
            sqlalch_td.Float:        decimal.Decimal,
            sqlalch_td.Boolean:      decimal.Decimal,
            sqlalch_td.DECIMAL:      decimal.Decimal,
            sqlalch_td.BYTEINT:      decimal.Decimal,
            sqlalch_td.DATE:         datetime.date,
            sqlalch_td.TIME:         datetime.time,
            sqlalch_td.TIMESTAMP:    datetime.datetime,
            sqlalch_td.Interval:     datetime.datetime,
            sqlalch_td.CHAR:         str,
            sqlalch_td.VARCHAR:      str,
            sqlalch_td.CLOB:         str,
            sqlalch_td.Text:         str,
            sqlalch_td.Unicode:      str,
            sqlalch_td.UnicodeText:  str
        }

        res = self.conn.execute(table.select())
        for col in res._cursor_description():
            assert(type_map[col_to_type[col[0]]] == col[1])

    def test_types_sqlalch_reflect(self):
        cols = [Column('column_' + str(i), type)
            for i, type in enumerate(self.sqlalch_types)]
        table = Table('table_test_types_sqlalch', self.metadata, *cols)
        self.metadata.create_all()

        col_to_type = {col.name: type(col.type) for col in cols}
        type_map    = {
            sqlalch_td.Integer:      sql.sqltypes.INTEGER,
            sqlalch_td.SmallInteger: sql.sqltypes.SMALLINT,
            sqlalch_td.BigInteger:   sql.sqltypes.BIGINT,
            sqlalch_td.Float:        sql.sqltypes.FLOAT,
            sqlalch_td.Boolean:      sqlalch_td.BYTEINT,
            sqlalch_td.DECIMAL:      sqlalch_td.DECIMAL,
            sqlalch_td.BYTEINT:      sqlalch_td.BYTEINT,
            sqlalch_td.DATE:         sqlalch_td.DATE,
            sqlalch_td.TIME:         sqlalch_td.TIME,
            sqlalch_td.TIMESTAMP:    sqlalch_td.TIMESTAMP,
            sqlalch_td.Interval:     sqlalch_td.TIMESTAMP,
            sqlalch_td.CHAR:         sqlalch_td.CHAR,
            sqlalch_td.VARCHAR:      sqlalch_td.VARCHAR,
            sqlalch_td.CLOB:         sqlalch_td.CLOB,
            sqlalch_td.Text:         sqlalch_td.CLOB,
            sqlalch_td.Unicode:      sqlalch_td.VARCHAR,
            sqlalch_td.UnicodeText:  sqlalch_td.CLOB
        }

        reflected_cols = self.inspect.get_columns('table_test_types_sqlalch')
        for col in reflected_cols:
            assert(type_map[col_to_type[col['name']]] == type(col['type']))

    def test_types_sqlalch_show(self):
        cols = [Column('column_' + str(i), type)
            for i, type in enumerate(self.sqlalch_types)]
        table = Table('table_test_types_sqlalch', self.metadata, *cols)
        self.metadata.create_all()

        col_to_type = {col.name: type(col.type) for col in cols}
        type_map    = {
            sqlalch_td.Integer:      'INTEGER',
            sqlalch_td.SmallInteger: 'SMALLINT',
            sqlalch_td.BigInteger:   'BIGINT',
            sqlalch_td.Float:        'FLOAT',
            sqlalch_td.Boolean:      'BYTEINT',
            sqlalch_td.DECIMAL:      'DECIMAL',
            sqlalch_td.BYTEINT:      'BYTEINT',
            sqlalch_td.DATE:         'DATE',
            sqlalch_td.TIME:         'TIME',
            sqlalch_td.TIMESTAMP:    'TIMESTAMP',
            sqlalch_td.Interval:     'TIMESTAMP',
            sqlalch_td.CHAR:         'CHAR',
            sqlalch_td.VARCHAR:      'VARCHAR',
            sqlalch_td.CLOB:         'CLOB',
            sqlalch_td.Text:         'CLOB',
            sqlalch_td.Unicode:      'VARCHAR',
            sqlalch_td.UnicodeText:  'CLOB'
        }

        parsed_attrs = utils.parse_show_table_col_attrs(
            self.conn.execute(
                'SHOW TABLE table_test_types_sqlalch').fetchone().items()[0][1],
            tuple(col.name for col in cols))

        for col, attr in parsed_attrs.items():
            assert(type_map[col_to_type[col]] in attr)

    def test_types_rawsql_select(self):
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
        stmt = 'CREATE TABLE table_test_types_rawsql (' +\
               ', '.join(['column_' + str(i) + ' ' + str(type) for
               i, type in enumerate(self.rawsql_types)]) + ')'
        Table('table_test_types_rawsql', self.metadata)
        self.conn.execute(stmt)

        col_to_type = {'column_' + str(i): type for
            i, type in enumerate(self.rawsql_types)}
        type_map    = {
            'BIGINT':       sql.sqltypes.BIGINT,
            'SMALLINT':     sql.sqltypes.SMALLINT,
            'BYTEINT':      sqlalch_td.BYTEINT,
            'INTEGER':      sql.sqltypes.INTEGER,
            'DECIMAL':      sqlalch_td.DECIMAL,
            'FLOAT':        sql.sqltypes.FLOAT,
            'NUMBER':       sqlalch_td.types.NUMERIC,
            'DATE':         sqlalch_td.DATE,
            'TIME':         sqlalch_td.TIME,
            'TIMESTAMP':    sqlalch_td.TIMESTAMP,
            'CHARACTER':    sqlalch_td.CHAR,
            'VARCHAR(50)':  sqlalch_td.VARCHAR,
            'CLOB':         sqlalch_td.CLOB
        }

        reflected_cols = self.inspect.get_columns('table_test_types_rawsql')
        for col in reflected_cols:
            assert(type_map[col_to_type[col['name']]] == type(col['type']))

    def test_types_rawsql_show(self):
        stmt = 'CREATE TABLE table_test_types_rawsql (' +\
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
