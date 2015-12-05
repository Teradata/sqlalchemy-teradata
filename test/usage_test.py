from sqlalchemy import *
from sqlalchemy.dialects import registry
from sqlalchemy_teradata.dialect import TeradataDialect
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.plugin.pytestplugin import *
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, Sequence

registry.register("tdalchemy", "sqlalchemy_teradata.dialect", "TeradataDialect")


class DialectSQLAlchUsageTest(fixtures.TestBase):

    def setup(self):
        self.dialect = TeradataDialect()
        self.engine = create_engine('tdalchemy://<db user>:<db pass>@<hostname:port>/dbc')
        self.conn = self.engine.connect()

        # build a table with columns
        self.metadata = MetaData()
        self.users = Table('my_users', self.metadata,
                           Column('uid', Integer, Sequence('uid_seq'), primary_key=True),
                           Column('name', String(256)),
                           Column('fullname', String(256))
                           )

        self.addresses = Table('addresses', self.metadata,
                               Column('id', Integer, Sequence('aid_seq'), primary_key=True),
                               Column('user_id', None, ForeignKey('my_users.uid')),
                               Column('email_address', String(256), nullable=False)
                               )

        self.metadata.create_all(self.engine)

    def tearDown(self):
        # self.metadata.drop_all(self.engine)
        self.conn.close()

    # pytest captures stdout by default pass -s to disable
    def test_show_state(self):
        print('setting up state:')
        for t in self.metadata.sorted_tables:
            print t

    def test_inserts(self):
        self.ins = self.users.insert()

        # inserts by default require all columns to be provided
        assert(str(self.ins), 'INSERT INTO my_users (uid, name, fullname) VALUES (:uid, :name, :fullname)')

        # use the VALUES claust to limit the values inserted
        self.ins = self.users.insert().values(name='mark', fullname='mark sandan')

        # actually values don't get stored in the string
        assert(str(self.ins), 'INSERT INTO users (name, fullname) VALUES (:name, :fullname)')

        # data values are stored in the INSERT but only are used when executed, we can peek
        assert(str(self.ins.compile().params), str({'fullname': 'mark sandan', 'name': 'mark'}))

    def test_executing(self):
        # re-create a new INSERT object
        self.ins = self.users.insert()
        # self.ins = self.users.insert().values(uid=100, name='mark', fullname='mark sandan')

        # execute the insert statement
        res = self.conn.execute(self.ins, uid=100, name='mark', fullname='mark sandan')

        print res.inserted_primary_key
