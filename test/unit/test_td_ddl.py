from sqlalchemy import Table, Column, Index, text
from sqlalchemy import MetaData, create_engine
from sqlalchemy import sql
from sqlalchemy.sql import operators
from sqlalchemy.schema import CreateColumn, CreateTable, CreateIndex, CreateSchema
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.plugin.pytestplugin import *
from sqlalchemy_teradata.types import ( VARCHAR, CHAR, CLOB, NUMERIC, DECIMAL )
from sqlalchemy_teradata.compiler import TDCreateTablePost, TDCreateTableSuffix

from itertools import product
import datetime as dt

"""
Unit testing for DDL Expressions and Dialect Extensions
The tests are based of off SQL Data Definition Language (Release 15.10, Dec '15)
"""

class TestCompileCreateColDDL(fixtures.TestBase):

    def setup(self):
        # Test locally for now
        def dump(sql, *multiaprams, **params):
          print(sql.compile(dialect=self.td_engine.dialect))

        self.td_engine = create_engine('teradata://', strategy='mock', executor=dump)
        self.sqlalch_col_attrs = ['primary_key', 'unique', 'nullable', 'default', 'index']

    def test_create_column(self):
        c = Column('column_name', VARCHAR(20, charset='GRAPHIC'))

    @pytest.mark.xfail
    def test_col_attrs(self):
        assert False

    @pytest.mark.xfail
    def test_col_add_attribute(self):
        assert False


class TestCompileCreateTableDDL(fixtures.TestBase):

    def setup(self):
        def dump(sql, *multiparams, **params):
            print(sql.compile(dialect=self.td_engine.dialect))
        self.td_engine = create_engine('teradata://', strategy='mock', executor=dump)

    def test_create_table(self):
        meta = MetaData(bind = self.td_engine)
        my_table = Table('tablename', meta,
                        Column('column1', NUMERIC, primary_key=True),
                        schema='database_name_or_user_name',
                        prefixes=['multiset', 'global temporary'])

    @pytest.mark.xfail
    def test_reflect_table(self):
        assert False


class TestCompileCreateIndexDDL(fixtures.TestBase):

    def setup(self):
        def dump(sql, *multiparams, **params):
            self.last_compiled = str(sql.compile(dialect=self.td_engine.dialect))
        self.td_engine = create_engine('teradata://', strategy='mock', executor=dump)
        self.metadata  = MetaData(bind=self.td_engine)

    def test_create_index_inline(self):
        """
        Tests SQL compilation of inline (column) CREATE INDEX.
        """
        my_table = Table('tablename', self.metadata,
                        Column('columnname', NUMERIC, index=True))
        self.metadata.create_all()

        assert(self.last_compiled ==
            'CREATE INDEX ix_tablename_columnname (columnname) ON tablename')

    def test_create_index_construct(self):
        """
        Tests SQL compilation of explicit CREATE INDEX with the SQLAlchemy
        Index schema construct.
        """
        my_table = Table('tablename', self.metadata,
                        Column('columnname', NUMERIC))
        Index('indexname', my_table.c.columnname)
        self.metadata.create_all()

        assert(self.last_compiled ==
            'CREATE INDEX indexname (columnname) ON tablename')

    def test_create_multiple_indexes(self):
        """
        Tests SQL compilation of CREATE INDEX with multiple indexes.
        """
        my_table = Table('tablename', self.metadata,
                        Column('column1', NUMERIC),
                        Column('column2', DECIMAL))
        Index('indexname', my_table.c.column1, my_table.c.column2)
        self.metadata.create_all()

        assert(self.last_compiled ==
            'CREATE INDEX indexname (column1, column2) ON tablename')

    def test_create_index_unique(self):
        """
        Tests SQL compilation of CREATE INDEX with a unique index.
        """
        my_table = Table('tablename', self.metadata,
                        Column('columnname', NUMERIC, index=True, unique=True))
        self.metadata.create_all()

        assert(self.last_compiled ==
            'CREATE UNIQUE INDEX ix_tablename_columnname (columnname) ON tablename')

    def test_create_multiple_unique_indexes(self):
        """
        Tests SQL compilation of CREATE INDEX with multiple unique indexes.
        """
        my_table = Table('tablename', self.metadata,
                        Column('column1', NUMERIC),
                        Column('column2', DECIMAL))
        Index('indexname', my_table.c.column1, my_table.c.column2, unique=True)
        self.metadata.create_all()

        assert(self.last_compiled ==
            'CREATE UNIQUE INDEX indexname (column1, column2) ON tablename')

    def test_create_index_noname(self):
        """
        Tests SQL compilation of CREATE INDEX with no name (explicitly passing
        None to the Index constructor).
        """
        my_table = Table('tablename', self.metadata,
                        Column('columnname', NUMERIC))
        Index(None, my_table.c.columnname)
        self.metadata.create_all()

        assert(self.last_compiled ==
            'CREATE INDEX ix_tablename_columnname (columnname) ON tablename')


class TestCompileSuffixDDL(fixtures.TestBase):

    def setup(self):
        def dump(sql, *multiparams, **params):
            self.last_compiled = str(sql.compile(dialect=self.td_engine.dialect))
        self.td_engine = create_engine('teradata://', strategy='mock', executor=dump)
        self.metadata  = MetaData(bind=self.td_engine)

    def test_create_suffix(self):
        """
        Tests SQL compilation of CREATE TABLE with (all) teradata dialect
        specific suffixes.
        """
        my_table = Table('tablename', self.metadata,
            Column('columnname', NUMERIC),
            teradata_suffixes=
                TDCreateTableSuffix().fallback() \
                                      .log() \
                                      .with_journal_table('anothertablename') \
                                      .before_journal() \
                                      .after_journal() \
                                      .checksum() \
                                      .freespace() \
                                      .no_mergeblockratio() \
                                      .mergeblockratio() \
                                      .min_datablocksize() \
                                      .max_datablocksize() \
                                      .blockcompression() \
                                      .with_no_isolated_loading(True) \
                                      .with_isolated_loading(True, 'all'))
        self.metadata.create_all()

        suffix_ddl = self.last_compiled[
            self.last_compiled.index(',')+1:self.last_compiled.index('(')]
        assert(suffix_ddl ==
            '\nfallback,' \
            '\nlog,' \
            '\nwith journal table=anothertablename,' \
            '\ndual before journal,' \
            '\nnot local after journal,' \
            '\nchecksum=default,' \
            '\nfreespace=0,' \
            '\nno mergeblockratio,' \
            '\ndefault mergeblockratio,' \
            '\nminimum datablocksize,' \
            '\nmaximum datablocksize,' \
            '\nblockcompression=default,' \
            '\nwith no concurrent isolated loading,' \
            '\nwith concurrent isolated loading for all ')

class TestCompilePostCreateDDL(fixtures.TestBase):

    def setup(self):
        def dump(sql, *multiparams, **params):
            self.last_compiled = str(sql.compile(dialect=self.td_engine.dialect))
        self.td_engine = create_engine('teradata://', strategy='mock', executor=dump)
        self.metadata  = MetaData(bind=self.td_engine)

    def test_create_post_create(self):
        """
        Tests SQL compilation of CREATE TABLE with (all) teradata dialect
        specific post_creates.
        """
        partition_opts = (
            True,
            {
                'c1': True,
                'c2': False,
                'c3': None,
                'c4': False,
                'c5': True
            },
            {
                'd1': True,
                'd2': None,
                'd3': False
            }, 1)
        my_table = Table('tablename', self.metadata,
            Column('c1', NUMERIC),
            Column('c2', DECIMAL),
            Column('c3', VARCHAR),
            Column('c4', CHAR),
            Column('c5', CLOB),
            teradata_post_create=
                TDCreateTablePost().no_primary_index() \
                                   .primary_index('indexname', True, ['c1']) \
                                   .primary_amp('ampname', ['c2']) \
                                   .partition_by_col(*(partition_opts[:-1]), None)
                                   .partition_by_col_auto_compress(*partition_opts)
                                   .partition_by_col_no_auto_compress(*partition_opts)
                                   .unique_index('uniqueindexname', ['c2, c3']))
        self.metadata.create_all()

        post_create_ddl = self.last_compiled[self.last_compiled.index(')\n')+2:]
        assert(post_create_ddl ==
            'NO PRIMARY INDEX,' \
            '\nunique primary index indexname( c1 ),' \
            '\nprimary amp index ampname( c2 ),' \
            '\npartition by( column all but( ' \
                'column(c1) auto compress , ' \
                'column(c5) auto compress , ' \
                'column(c2) no auto compress, ' \
                'column(c4) no auto compress, ' \
                'column(c3), ' \
                'row(d1) auto compress, ' \
                'row(d3) no auto compress, ' \
                'row(d2) )),' \
            '\npartition by( column auto compress all but( ' \
                'column(c1) auto compress , ' \
                'column(c5) auto compress , ' \
                'column(c2) no auto compress, ' \
                'column(c4) no auto compress, ' \
                'column(c3), ' \
                'row(d1) auto compress, ' \
                'row(d3) no auto compress, ' \
                'row(d2) )' \
            'add 1 ),' \
            '\npartition by( column no auto compress all but( ' \
                'column(c1) auto compress , ' \
                'column(c5) auto compress , ' \
                'column(c2) no auto compress, ' \
                'column(c4) no auto compress, ' \
                'column(c3), ' \
                'row(d1) auto compress, ' \
                'row(d3) no auto compress, ' \
                'row(d2) )' \
            'add 1 ),' \
            '\nunique index uniqueindexname( c2, c3 )' \
            '\n\n')


class TestCompileOperators(fixtures.TestBase):

    def setup(self):
        def dump(sql, *multiparams, **params):
            self.last_compiled = str(sql.compile(dialect=self.td_engine.dialect))
        self.td_engine = create_engine('teradata://', strategy='mock', executor=dump)
        self.metadata  = MetaData(bind=self.td_engine)

        self.table = Table('t_test', self.metadata, Column('c1', NUMERIC))

    def test_compile_binary_operators(self):
        """
        Tests SQL compilation of a selection of binary operators.
        """
        op_map = {
            operators.and_:      ' AND ',
            operators.or_:       ' OR ',
            operators.add:       ' + ',
            operators.mul:       ' * ',
            operators.sub:       ' - ',
            operators.div:       ' / ',
            operators.mod:       ' MOD ',
            operators.truediv:   ' / ',
            operators.lt:        ' < ',
            operators.le:        ' <= ',
            operators.ne:        ' <> ',
            operators.gt:        ' > ',
            operators.ge:        ' >= ',
            operators.eq:        ' = ',
            operators.concat_op: ' || ',
            operators.like_op:   ' LIKE ',
            operators.is_:       ' IS ',
            operators.isnot:     ' IS NOT '
        }

        for op in op_map.keys():
            self.td_engine.execute(op(self.table.c.c1, text('arg')))

            assert(self.last_compiled == 't_test.c1' + op_map[op] + 'arg')

    def test_compile_in_operators(self):
        """
        Tests SQL compilation of the IN and NOT IN binary operators.
        """
        op_map = {
            operators.in_op:    ' IN ',
            operators.notin_op: ' NOT IN ',
        }

        for op in op_map.keys():
            self.td_engine.execute(op(self.table.c.c1, (0, 0)))

            assert(self.last_compiled ==  't_test.c1' + op_map[op] + '(?, ?)')

    def test_compile_unary_operators(self):
        """
        Tests SQL compilation of a selection of unary operators.
        """
        op_map = {
            operators.distinct_op: 'DISTINCT ',
            operators.inv:         'NOT '
        }

        for op in op_map.keys():
            self.td_engine.execute(op(self.table.c.c1))

            assert(self.last_compiled == op_map[op] + 't_test.c1')

    def test_compile_any_all_operators(self):
        """
        Tests SQL compilation of the ANY and ALL unary operators.
        """
        op_map = {
            operators.any_op: 'ANY ',
            operators.all_op: 'ALL ',
        }

        for op in op_map.keys():
            self.td_engine.execute(
                op(sql.select([self.table.c.c1]).as_scalar()))

            assert(self.last_compiled ==
                op_map[op] + '(SELECT t_test.c1 \nFROM t_test)')

    def test_compile_modifier_operators(self):
        """
        Tests SQL compilation of a selection of modifier operators.
        """
        op_map = {
            operators.desc_op:       ' DESC',
            operators.asc_op:        ' ASC',
            operators.nullsfirst_op: ' NULLS FIRST',
            operators.nullslast_op:  ' NULLS LAST',
        }

        for op in op_map.keys():
            self.td_engine.execute(op(self.table.c.c1))

            assert(self.last_compiled == 't_test.c1' + op_map[op])

    def test_compile_negative_operator(self):
        """
        Tests SQL compilation of the negative operator.
        """
        self.td_engine.execute(operators.neg(self.table.c.c1))

        assert(self.last_compiled == '-t_test.c1')

    def test_compile_nested_operators(self):
        """
        Tests SQL compilation of nested operators.
        """
        self.td_engine.execute(
            operators.and_(
                operators.ne(self.table.c.c1, 0),
                operators.mod(self.table.c.c1, 0)))

        assert(self.last_compiled == 't_test.c1 <> ? AND t_test.c1 MOD ?')
