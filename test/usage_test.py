from sqlalchemy import *
from sqlalchemy.dialects import registry
from sqlalchemy_teradata.dialect import TeradataDialect
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.plugin.pytestplugin import *
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey

registry.register("tdalchemy", "sqlalchemy_teradata.dialect", "TeradataDialect")


class DialectSQLAlchUsageTest(fixtures.TestBase):
    """ This usage test is meant to serve as documentation and follows the
        tutorial here: http://docs.sqlalchemy.org/en/latest/core/tutorial.html
        but with the dialect being developed
    """

    def setup(self):
        self.dialect = TeradataDialect()
        self.engine = create_engine('tdalchemy://<db user>:<db pass>@<hostname:port>/dbc')
        self.conn = self.engine.connect()

        # build a table with columns
        self.metadata = MetaData()
        self.users = Table('my_users', self.metadata,
                           Column('uid', Integer, primary_key=True),
                           Column('name', String(256)),
                           Column('fullname', String(256)),
                           tdalchemy_unique_pi='uid'  # optional if we set pk
                           )

        self.addresses = Table('addresses', self.metadata,
                               Column('id', Integer, primary_key=True),
                               Column('user_id', None, ForeignKey('my_users.uid')),
                               Column('email_address', String(256), nullable=False),
                               tdalchemy_unique_pi='id'  # optional if we set pk
                               )

        self.metadata.create_all(self.engine)

    def tearDown(self):
        self.metadata.drop_all(self.engine)
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

        # None of the inserts above in this test get added to the database

    def test_executing(self):
        # re-create a new INSERT object
        self.ins = self.users.insert()

        # execute the insert statement
        res = self.conn.execute(self.ins, uid=1, name='jack', fullname='Jack Jones')
        res = self.conn.execute(self.ins, uid=2, name='wendy', fullname='Wendy Williams')

        # the res variable is a ResultProxy object, analagous to DBAPI cursor
        print str(res.inserted_primary_key)

        # issue many inserts, the same is possible for update and delete
        self.conn.execute(self.addresses.insert(), [
             {'id': 1, 'user_id': 1, 'email_address': 'jack@yahoo.com'},
             {'id': 2, 'user_id': 1, 'email_address': 'jack@msn.com'},
             {'id': 3, 'user_id': 2, 'email_address': 'www@www.com'},
             {'id': 4, 'user_id': 2, 'email_address': 'wendy@aol.com'}
         ])

        # test selects on the inserted values
        from sqlalchemy.sql import select

        s = select([self.users])
        res = self.conn.execute(s)
        u1 = res.fetchone()
        u2 = res.fetchone()

        # accessing rows
        print("name:", u1['name'], " fullname:", u1['fullname'])
        print("name:", u2['name'], " fullname:", u2['fullname'])

        print("name:", u1[1], " fullname:", u1[2])
        print("name:", u2[1], " fullname:", u2[2])

        # be sure to close the result set
        res.close()

        # use cols to access rows
        for row in self.conn.execute(s):
            print("name:", row[self.users.c.name], "; fullname:", row[self.users.c.fullname])

        # reference individual columns in select clause
        s = select([self.users.c.name, self.users.c.fullname])
        res = self.conn.execute(s)
        for row in res:
            print(row)

        # test joins
        # cartesian product
        for row in self.conn.execute(select([self.users, self.addresses])):
            print(row)

        # inner join on id
        s = select([self.users, self.addresses]).where(self.users.c.uid == self.addresses.c.user_id)
        for row in self.conn.execute(s):
            print(row)

        # operators between columns objects & other col objects/literals
        expr = self.users.c.uid == self.addresses.c.user_id
        assert('my_users.uid = addresses.user_id', str(expr))
        # see how Teradata concats two strings
        assert((self.users.c.name + self.users.c.fullname).compile(bind=self.engine),
               'my_users.name || my_users.fullname')

        # built-in conjunctions
        from sqlalchemy.sql import and_, or_

        s = select([(self.users.c.fullname +
                     ", " +
                     self.addresses.c.email_address).label('titles')]).where(
                         and_(
                             self.users.c.uid == self.addresses.c.user_id,
                             self.users.c.name.between('m', 'z'),
                             or_(
                                 self.addresses.c.email_address.like('%@aol.com'),
                                 self.addresses.c.email_address.like('%@msn.com')
                             )
                         )
                     )
        print(s)
        res = self.conn.execute(s)
        for row in res:
            print(row)

        # more joins
        # ON condition auto generated based on ForeignKey
        assert(self.users.join(self.addresses),
               'my_users JOIN addresses ON my_users.uid = addresses.user_id')

        # specify the join ON condition
        self.users.join(self.addresses,
                        self.addresses.c.email_address.like(self.users.c.name + '%'))

        # select from clause to specify tables and the ON condition
        s = select([self.users.c.fullname]).select_from(
            self.users.join(self.addresses, self.addresses.c.email_address.like(self.users.c.name + '%')))
        res = self.conn.execute(s)
        assert(len(res.fetchall()), 3)

        # left outer joins
        s = select([self.users.c.fullname]).select_from(self.users.outerjoin(self.addresses))
        # outer join works with teradata dialect (unlike oracle dialect < version9)

        assert(s, s.compile(dialect=self.dialect))

        # test bind params (positional)

        from sqlalchemy import text, column
        s = self.users.select(self.users.c.name.like(
                                bindparam('username', type_=String)+text("'%'")))
        res = self.conn.execute(s, username='wendy').fetchall()
        assert(len(res), 1)

        # sql functions
        from sqlalchemy.sql import func

        # certain function names are known by sqlalchemy
        assert(str(func.current_timestamp()), 'CURRENT_TIMESTAMP')

        # functions can be used in the select
        res = self.conn.execute(select(
            [func.max(self.addresses.c.email_address, type_=String).label(
                'max_email')])).scalar()
        assert(res, 'www@www.com')

        # func result sets, define a function taking params x,y return q,z,r
        # useful for nested queries, subqueries - w/ dynamic params
        calculate = select([column('q'), column('z'), column('r')]).\
                       select_from(
                            func.calculate(
                                bindparam('x'),
                                bindparam('y')
                            )
                        )
        calc = calculate.alias()
        s = select([self.users]).where(self.users.c.uid > calc.c.z)
        assert('SELECT my_users.uid, my_users.name, my_users.fullname\
               FROM my_users, (SELECT q, z, r\
                               FROM calculate(:x, :y)) AS anon_1\
               WHERE my_users.uid > anon_1.z', s)
        # instantiate the func
        calc1 = calculate.alias('c1').unique_params(x=17, y=45)
        calc2 = calculate.alias('c2').unique_params(x=5, y=12)

        s = select([self.users]).where(self.users.c.uid.between(calc1.c.z, calc2.c.z))
        parms = s.compile().params
        print(parms)
        assert('x_2' in parms, 'x_1' in parms)
        assert('y_2' in parms, 'y_1' in parms)
        assert(parms['x_1'] == 17,parms['y_1'] == 45)
        assert(parms['x_2'] == 5,parms['y_2'] == 12)

        ## test union and except
        from sqlalchemy.sql import except_, union

        u = union(
            self.addresses.select().where(self.addresses.c.email_address == 'foo@bar.com'),
            self.addresses.select().where(self.addresses.c.email_address.like('%@yahoo.com')),)# .order_by(self.addresses.c.email_address)
        print(u)
        #res = self.conn.execute(u) this fails, syntax error order by expects pos integer?

        u = except_(
              self.addresses.select().where(self.addresses.c.email_address.like('%@%.com')),
              self.addresses.select().where(self.addresses.c.email_address.like('%@msn.com')))
        res = self.conn.execute(u).fetchall()
        assert(1, len(res))

        u = except_(
               union(
                  self.addresses.select().where(self.addresses.c.email_address.like('%@yahoo.com')),
                  self.addresses.select().where(self.addresses.c.email_address.like('%@msn.com'))
              ).alias().select(),
              self.addresses.select(self.addresses.c.email_address.like('%@msn.com'))
        )


        res = self.conn.execute(u).fetchall()
        assert(1, len(res))

        ## scalar subqueries
        stmt = select([func.count(self.addresses.c.id)]).where(self.users.c.uid == self.addresses.c.user_id).as_scalar()

        # we can place stmt as any other column within another select
        res = self.conn.execute(select([self.users.c.name, stmt])).fetchall()
        print(res)
        assert(2, len(res))

        # we can label the inner query
        stmt = select([func.count(self.addresses.c.id)]).\
            where(self.users.c.uid == self.addresses.c.user_id).\
            label("address_count")

        res = self.conn.execute(select([self.users.c.name, stmt])).fetchall()
        print(res)
        assert(2, len(res))


        ## inserts, updates, deletes
        stmt = self.users.update().values(fullname="Fullname: " +self.users.c.name)
        res = self.conn.execute(stmt)

        stmt = self.users.insert().values(name=bindparam('_name') + " .. name")
        res = self.conn.execute(stmt, [ {'uid':4, '_name':'name1'},
                                        {'uid':5, '_name':'name2'},
                                        {'uid':6, '_name':'name3'},])
        print(res)

        # updates
        stmt = self.users.update().where(self.users.c.name == 'jack').values(name = 'ed')
        res = self.conn.execute(stmt)
        print(res)

        # update many with bound params
        stmt = self.users.update().where(self.users.c.name == bindparam('oldname')).\
               values(name=bindparam('newname'))
        res = self.conn.execute(stmt, [
                   {'oldname':'jack', 'newname':'ed'},
                   {'oldname':'wendy', 'newname':'mary'},
                   {'oldname':'jim', 'newname':'jake'},
        ])

        # correlated updates
        stmt = select([self.addresses.c.email_address]).\
                   where(self.addresses.c.user_id == self.users.c.uid).\
                   limit(1)
        # this fails, syntax error bc of LIMIT - need TOP/SAMPLE instead
        #res = self.conn.execute(self.users.update().values(fullname=stmt))

        # multiple table updates
        stmt = self.users.update().\
               values(name='ed wood').\
               where(self.users.c.uid == self.addresses.c.id).\
               where(self.addresses.c.email_address.startswith('ed%'))

        # this fails, teradata does update from set where not update set from where
        #res = self.conn.execute(stmt)

        stmt = self.users.update().\
                 values({
                     self.users.c.name:'ed wood',
                     self.addresses.c.email_address:'ed.wood@foo.com'
                 }).\
                 where(self.users.c.uid == self.addresses.c.id).\
                 where(self.addresses.c.email_address.startswith('ed%'))
        # fails but works on MySQL, should this work for us?
        #res = self.conn.execute(stmt)

        # deletes
        self.conn.execute(self.addresses.delete())
        self.conn.execute(self.users.delete().where(self.users.c.name > 'm'))

        # matched row counts
        # updates + deletes have a number indicating # rows matched by WHERE clause
        res = self.conn.execute(self.users.delete())
        assert(res.rowcount, 1)
