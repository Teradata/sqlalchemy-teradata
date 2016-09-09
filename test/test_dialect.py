from sqlalchemy_teradata.dialect import TeradataDialect
from sqlalchemy.testing import fixtures
from sqlalchemy import testing, select
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy_teradata.base import CreateView,DropView

class TeradataDialectTest(fixtures.TestBase):

    def setup(self):
        self.conn = testing.db.connect()
        self.engine = self.conn.engine
        self.dialect = self.conn.dialect
        self.metadata = MetaData()

        self.user_name = self.engine.execute('sel user').scalar()
        self.tbl_name = self.user_name + '_test_table'
        self.view_name = self.user_name + '_test_view'

        # Setup test table (user should have necessary rights to create table)
        self.test_table = Table(self.tbl_name, self.metadata,
                                Column('id', Integer, primary_key=True))
        # Setup a test view 
        self.test_view = CreateView(self.view_name, select([self.test_table.c.id.label('view_id')]))

        # Create tables
        self.metadata.create_all(self.engine)

        #Create views
        self.conn.execute(self.test_view)

    def tearDown(self):
        # drop view(s)
        self.conn.execute(DropView(self.view_name))

        # drop table(s)
        self.metadata.drop_all(self.engine)
        self.conn.close()

    def test_has_table(self):
        assert self.dialect.has_table(self.conn, self.test_table.name)

    def test_get_table_names(self):
        tbls = self.dialect.get_table_names(self.conn)
        assert self.dialect.normalize_name(self.test_table.name) in tbls

    def test_get_schema_names(self):
        schemas = self.dialect.get_schema_names(self.conn)
        assert self.dialect.normalize_name(self.user_name) in schemas

    def test_get_view_names(self):
        views = self.dialect.get_view_names(self.conn)
        assert self.dialect.normalize_name(self.test_view.name) in views

    def test_get_pk_constraint(self):
        assert False

    def test_get_unique_constraint(self):
        assert False

    def test_get_foreign_keys(self):
        assert False

    def test_get_indexes(self):
        assert False
