from sqlalchemy import *
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import testing
from sqlalchemy.engine import reflection
# from sqlalchemy.schema import CreateColumn, CreateTable, CreateIndex, CreateSchema
from sqlalchemy.testing.plugin.pytestplugin import *

import sqlalchemy_teradata as sqlalch_td
import sqlalchemy.sql as sql
import decimal, datetime

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

    def tearDown(self):
        self.metadata.drop_all(self.engine)
        self.conn.close()

    def test_types_sqlalch_select(self):
        cols  = [Column('column_' + str(i), type)
            for i, type in enumerate(sqlalch_td.__all__)]
        table = Table('table_test_types_sqlalch', self.metadata, *cols)
        self.metadata.create_all(self.engine, tables=[table])

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
            for i, type in enumerate(sqlalch_td.__all__)]
        table = Table('table_test_types_sqlalch', self.metadata, *cols)
        self.metadata.create_all(self.engine, tables=[table])

        col_to_type = {col.name: type(col.type) for col in cols}

        reflected_cols = self.inspect.get_columns('table_test_types_sqlalch')
        for col in reflected_cols:
            print(col_to_type[col['name']], type(col['type']))

    def test_types_rawsql_select(self):
        types = ['CHARACTER', 'VARCHAR(50)', 'CLOB', 'BIGINT', 'SMALLINT',
                 'BYTEINT', 'INTEGER', 'DECIMAL', 'FLOAT', 'NUMBER',
                 'DATE', 'TIME', 'TIMESTAMP']
        sql   = 'CREATE TABLE table_test_types_rawsql (' +\
                ', '.join(['column_' + str(i) + ' ' + str(type) for
                i, type in enumerate(types)]) + ')'
        Table('table_test_types_rawsql', self.metadata)
        self.conn.execute(sql)

        col_to_type = {'column_' + str(i): type for i, type in enumerate(types)}
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
        types = ['CHARACTER', 'VARCHAR(50)', 'CLOB', 'BIGINT', 'SMALLINT',
                 'BYTEINT', 'INTEGER', 'DECIMAL', 'FLOAT', 'NUMBER',
                 'DATE', 'TIME', 'TIMESTAMP']
        sql   = 'CREATE TABLE table_test_types_rawsql (' +\
                ', '.join(['column_' + str(i) + ' ' + str(type) for
                i, type in enumerate(types)]) + ')'
        Table('table_test_types_rawsql', self.metadata)
        self.conn.execute(sql)

        col_to_type = {'column_' + str(i): type for i, type in enumerate(types)}

        reflected_cols = self.inspect.get_columns('table_test_types_rawsql')
        for col in reflected_cols:
            print(col_to_type[col['name']], type(col['type']))
