from sqlalchemy import Table, Column, Index
from sqlalchemy import MetaData, create_engine
from sqlalchemy.schema import CreateColumn, CreateTable, CreateIndex, CreateSchema
from sqlalchemy.testing import fixtures
from sqlalchemy_teradata.types import ( VARCHAR, CHAR, CLOB, NUMERIC, DECIMAL )
from sqlalchemy_teradata.compiler import TDCreateTablePost, TDCreateTablePostfix

from itertools import product
import datetime as dt

"""
Test DDL Expressions and Dialect Extensions
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

  def test_col_attrs(self):
    assert False

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

    def test_reflect_table(self):
        assert False


class TestCompileCreateIndexDDL(fixtures.TestBase):

    def setup(self):
        def dump(sql, *multiparams, **params):
            self.last_compiled = str(sql.compile(dialect=self.td_engine.dialect))
        self.td_engine = create_engine('teradata://', strategy='mock', executor=dump)
        self.metadata  = MetaData(bind=self.td_engine)

    def test_create_index_inline(self):
        my_table = Table('tablename', self.metadata,
                        Column('columnname', NUMERIC, index=True))
        self.metadata.create_all()

        assert(self.last_compiled ==
            'CREATE INDEX ix_tablename_columnname (columnname) ON tablename')

    def test_create_index_construct(self):
        my_table = Table('tablename', self.metadata,
                        Column('columnname', NUMERIC))
        Index('indexname', my_table.c.columnname)
        self.metadata.create_all()

        assert(self.last_compiled ==
            'CREATE INDEX indexname (columnname) ON tablename')

    def test_create_multiple_indexes(self):
        my_table = Table('tablename', self.metadata,
                        Column('column1', NUMERIC),
                        Column('column2', DECIMAL))
        Index('indexname', my_table.c.column1, my_table.c.column2)
        self.metadata.create_all()

        assert(self.last_compiled ==
            'CREATE INDEX indexname (column1, column2) ON tablename')

    def test_create_index_unique(self):
        my_table = Table('tablename', self.metadata,
                        Column('columnname', NUMERIC, index=True, unique=True))
        self.metadata.create_all()

        assert(self.last_compiled ==
            'CREATE UNIQUE INDEX ix_tablename_columnname (columnname) ON tablename')

    def test_create_index_noname(self):
        my_table = Table('tablename', self.metadata,
                        Column('columnname', NUMERIC))
        Index(None, my_table.c.columnname)
        self.metadata.create_all()

        assert(self.last_compiled ==
            'CREATE INDEX ix_tablename_columnname (columnname) ON tablename')


class TestCompileCreateSuffixDDL(fixtures.TestBase):

    def setup(self):
        def dump(sql, *multiparams, **params):
            self.last_compiled = str(sql.compile(dialect=self.td_engine.dialect))
        self.td_engine = create_engine('teradata://', strategy='mock', executor=dump)
        self.metadata  = MetaData(bind=self.td_engine)

    def test_create_suffix(self):
        my_table = Table('tablename', self.metadata,
            Column('columnname', CLOB),
            teradata_postfixes=
                TDCreateTablePostfix().fallback() \
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
                                      .with_no_isolated_loading() \
                                      .with_concurrent_isolated_loading('none'))
        self.metadata.create_all()

        postfix_ddl = self.last_compiled[
            self.last_compiled.index(',')+1:self.last_compiled.index('(')]
        assert(postfix_ddl ==
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
            '\nwith no isolated loading,' \
            '\nwith concurrent isolated loading for none=none ')

class TestCompileCreateTablePostDDL(fixtures.TestBase):

    def setup(self):
        def dump(sql, *multiparams, **params):
            self.last_compiled = str(sql.compile(dialect=self.td_engine.dialect))
        self.td_engine = create_engine('teradata://', strategy='mock', executor=dump)
        self.metadata  = MetaData(bind=self.td_engine)

    def test_create_table_post(self):
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
                                   .partition_by_col(
                                        cols={
                                            'c1': True,
                                            'c2': False,
                                            'c3': None,
                                            'c4': False,
                                            'c5': True
                                        },
                                        rows={
                                            'd1': True,
                                            'd2': None,
                                            'd3': False
                                        },
                                        const=1
                                   ))

        self.metadata.create_all()

        post_create_ddl = self.last_compiled[self.last_compiled.index(')\n')+2:]
        assert(post_create_ddl ==
            'NO PRIMARY INDEX,' \
            '\nunique primary index indexname( c1 ),' \
            '\nprimary amp index ampname( c2 ),' \
            '\npartition by( column( ' \
                'column(c1) auto compress , ' \
                'column(c5) auto compress , ' \
                'column(c2) no auto compress, ' \
                'column(c4) no auto compress, ' \
                'column(c3), ' \
                'row(d1) auto compress, ' \
                'row(d3) no auto compress, ' \
                'row(d2) )' \
            'add 1 )\n\n')
