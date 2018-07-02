from sqlalchemy import Table, Column, Index
from sqlalchemy.schema import CreateColumn, CreateTable, CreateIndex, CreateSchema
from sqlalchemy import MetaData, create_engine
from sqlalchemy_teradata.types import ( VARCHAR, CHAR, CLOB, NUMERIC, DECIMAL )
from sqlalchemy.testing import fixtures

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

    def test_create_indexes(self):
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
