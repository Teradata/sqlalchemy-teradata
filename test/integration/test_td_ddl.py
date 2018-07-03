from sqlalchemy import *
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import testing
from sqlalchemy.engine import reflection
from sqlalchemy.testing.plugin.pytestplugin import *
from sqlalchemy_teradata.compiler import TDCreateTablePost, TDCreateTablePostfix

import sqlalchemy_teradata as sqlalch_td
import sqlalchemy.sql as sql
import decimal, datetime

import utils, itertools

"""
Test DDL Expressions and Dialect Extensions
The tests are based of off SQL Data Definition Language (Release 15.10, Dec '15)
"""

class TestCreateTableDDL(testing.fixtures.TestBase):

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
        self.metadata.create_all(checkfirst=False)

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
        self.metadata.create_all(checkfirst=False)

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
        self.metadata.create_all(checkfirst=False)

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


class TestCreateSuffixDDL(testing.fixtures.TestBase):

    def setup(self):
        self.conn     = testing.db.connect()
        self.engine   = self.conn.engine
        self.metadata = MetaData(bind=self.engine)

    def tearDown(self):
        self.metadata.drop_all(self.engine)
        self.conn.invalidate()
        self.conn.close()

    def test_create_suffix_fallback(self):
        Table('t_no_fallback', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().fallback(enabled=False))

        Table('t_fallback', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().fallback(enabled=True))

        self.metadata.create_all(checkfirst=False)
        assert(self.engine.has_table('t_no_fallback'))
        assert(self.engine.has_table('t_fallback'))

    def test_create_suffix_log(self):
        Table('t_log', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().log(enabled=True))

        self.metadata.create_all(checkfirst=False)
        assert(self.engine.has_table('t_log'))

    @pytest.mark.xfail(strict=True)
    def test_create_suffix_nolog_err(self):
        Table('t_nolog_invalid', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().log(enabled=False))

        self.metadata.create_all(checkfirst=False)

    # TODO
    # def test_create_suffix_journal(self):

    def test_create_suffix_checksum(self):
        Table('t_checksum_default', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=
                TDCreateTablePostfix().checksum())

        Table('t_checksum_off', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=
                TDCreateTablePostfix().checksum(integrity_checking='off'))

        Table('t_checksum_on', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=
                TDCreateTablePostfix().checksum(integrity_checking='on'))

        self.metadata.create_all(checkfirst=False)
        assert(self.engine.has_table('t_checksum_default'))
        assert(self.engine.has_table('t_checksum_off'))
        assert(self.engine.has_table('t_checksum_on'))

    def test_create_suffix_freespace(self):
        Table('t_freespace_0', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().freespace())

        Table('t_freespace_75', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().freespace(percentage=75))

        Table('t_freespace_40', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().freespace(percentage=40))

        self.metadata.create_all(checkfirst=False)
        assert(self.engine.has_table('t_freespace_0'))
        assert(self.engine.has_table('t_freespace_75'))
        assert(self.engine.has_table('t_freespace_40'))

    @pytest.mark.xfail(strict=True)
    def test_create_suffix_freespace_err(self):
        Table('t_freespace_invalid', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().freespace(percentage=100))

        self.metadata.create_all(checkfirst=False)

    def test_create_suffix_mergeblockratio(self):
        Table('t_default_mergeblockratio', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().mergeblockratio())

        Table('t_mergeblockratio_0', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().mergeblockratio(integer=0))

        Table('t_mergeblockratio_100', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().mergeblockratio(integer=100))

        Table('t_mergeblockratio_50', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().mergeblockratio(integer=50))

        Table('t_no_mergeblockratio', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().no_mergeblockratio())

        self.metadata.create_all(checkfirst=False)
        assert(self.engine.has_table('t_default_mergeblockratio'))
        assert(self.engine.has_table('t_mergeblockratio_0'))
        assert(self.engine.has_table('t_mergeblockratio_100'))
        assert(self.engine.has_table('t_mergeblockratio_50'))
        assert(self.engine.has_table('t_no_mergeblockratio'))

    @pytest.mark.xfail(strict=True)
    def test_create_suffix_mergeblockratio_err(self):
        Table('t_mergeblockratio_invalid', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().freespace(percentage=101))

        self.metadata.create_all(checkfirst=False)

    def test_create_suffix_datablocksize(self):
        Table('t_min_datablocksize', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().min_datablocksize())

        Table('t_max_datablocksize', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().max_datablocksize())

        Table('t_default_datablocksize', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=TDCreateTablePostfix().datablocksize())

        Table('t_custom_datablocksize', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=
                TDCreateTablePostfix().datablocksize(data_block_size=21248))

        self.metadata.create_all(checkfirst=False)
        assert(self.engine.has_table('t_min_datablocksize'))
        assert(self.engine.has_table('t_max_datablocksize'))
        assert(self.engine.has_table('t_default_datablocksize'))
        assert(self.engine.has_table('t_custom_datablocksize'))

    @pytest.mark.xfail(strict=True)
    def test_create_suffix_datablocksize_err(self):
        Table('t_datablocksize_invalid', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=
                TDCreateTablePostfix().datablocksize(data_block_size=1024))

        self.metadata.create_all(checkfirst=False)

    def test_create_suffix_blockcompression(self):
        Table('t_blockcompression_default', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=
                TDCreateTablePostfix().blockcompression())

        Table('t_blockcompression_autotemp', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=
                TDCreateTablePostfix().blockcompression(opt='autotemp'))

        Table('t_blockcompression_manual', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=
                TDCreateTablePostfix().blockcompression(opt='manual'))

        Table('t_blockcompression_never', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=
                TDCreateTablePostfix().blockcompression(opt='never'))

        self.metadata.create_all(checkfirst=False)
        assert(self.engine.has_table('t_blockcompression_default'))
        assert(self.engine.has_table('t_blockcompression_autotemp'))
        assert(self.engine.has_table('t_blockcompression_manual'))
        assert(self.engine.has_table('t_blockcompression_never'))

    def test_create_suffix_isolated_loading(self):
        Table('t_no_isolated_loading', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=
                TDCreateTablePostfix().with_no_isolated_loading(concurrent=False))

        Table('t_no_isolated_loading_concurrent', self.metadata,
            Column('c', NUMERIC),
            teradata_postfixes=
                TDCreateTablePostfix().with_no_isolated_loading(concurrent=True))

        for_opts        = ('all', 'insert', 'none', None)
        concurrent_opts = (True, False)
        for concurrent, opt in itertools.product(concurrent_opts, for_opts):
            Table(
                't_isolated_loading' +\
                    ('_concurrent' if concurrent else '') +\
                    ('_' + opt if opt is not None else ''),
                self.metadata,
                Column('c', NUMERIC),
                teradata_postfixes=TDCreateTablePostfix().with_isolated_loading(
                    concurrent=concurrent, opt=opt))

        self.metadata.create_all(checkfirst=False)
        assert(self.engine.has_table('t_no_isolated_loading'))
        assert(self.engine.has_table('t_no_isolated_loading_concurrent'))
        for concurrent, opt in itertools.product(concurrent_opts, for_opts):
            # TODO figure out a better way to bypass SQL_ACTIVE_STATEMENTS limit
            self.conn.invalidate()
            self.conn.close()
            self.conn = testing.db.connect()
            assert(self.engine.has_table(
                't_isolated_loading' +\
                    ('_concurrent' if concurrent else '') +\
                    ('_' + opt if opt is not None else '')))
