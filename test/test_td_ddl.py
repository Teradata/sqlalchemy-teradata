from sqlalchemy import Table, Column, Index
from sqlalchemy import CreateColumn, CreateTable, CreateIndex, CreateSchema
from sqlalchemy import Metadata, create_engine
from sqlalchemy_teradata import ( Varchar, Char, Clob )
from sqlalchemy_teradata import ( Integer, Decimal, )
from sqlalchemy_teradata import ( Date, Time, Timestamp )
from sqlalchemy.testing import fixtures

from itertools import product
import datetime as dt

"""
Test DDL Expressions and Dialect Extensions
The tests are based of off SQL Data Definition Language (Release 15.10, Dec '15)
"""

class TestCompileCreateColDDL(fixtures.TestBase):

  def setup(self):

    def dump(sql, *multiaprams, **params):
      print(sql.compile(dialect=td_engine.dialect))

    self.td_engine = create_engine('teradata://', strategy='mock', executor=dump)

    self.sqlalch_col_attrs = ['primary_key', 'unique', 'nullable', 'default',
                              'index', 

  def test_create_column(self):
    c = Column('column_name', Varchar(20, charset='GRAPHIC'))

  def test_col_attrs(self):
    pass

  def test_col_add_attribute(self):
    pass


class TestCompileCreateTableDDL(fixtures.TestBase):

	def setup(self):

    def dump(sql, *multiparams, **params):
      print(sql.compile(dialect=td_engine.dialect))

    self.td_engine = create_engine('teradata://', strategy='mock', executor=dump)

	def test_create_table(self):

    meta = MetaData(bind = td_engine)
    my_table = Table('tablename', meta,
                        Column('column1', Integer, primary_key=True),
                        schema='database_name_or_user_name',
                        prefixes=['multiset', 'global temporary']
                    )

  def test_reflect_table(self):
    pass


