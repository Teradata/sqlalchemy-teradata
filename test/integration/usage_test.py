from sqlalchemy import *
from sqlalchemy import testing
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy.dialects import registry
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relation, sessionmaker
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.plugin.pytestplugin import *
from sqlalchemy_teradata.dialect import TeradataDialect

class DialectSQLAlchUsageTest(fixtures.TestBase):
    """ This usage test is meant to serve as documentation and follows the
        tutorial here: http://docs.sqlalchemy.org/en/latest/core/tutorial.html
        but with the dialect being developed
    """

    # Note: this test uses pytest which captures stdout by default, pass -s to allow output to stdout

    def setUp(self):

        self.dialect = TeradataDialect()
        self.conn = testing.db.connect()
        self.engine = self.conn.engine

        # build a table with columns
        self.metadata = MetaData()
        self.users = Table('my_users', self.metadata,
                           Column('uid', Integer, primary_key=True),
                           Column('name', String(256)),
                           Column('fullname', String(256)),
                           )

        self.addresses = Table('addresses', self.metadata,
                               Column('id', Integer, primary_key=True),
                               Column('user_id', None, ForeignKey('my_users.uid'), nullable=False),
                               Column('email_address', String(256), nullable=False),
                               )

        self.metadata.create_all(self.engine)

    def tearDown(self):
        self.metadata.drop_all(self.engine)
        self.conn.close()

    def test_show_state(self):
        assert self.users in self.metadata.sorted_tables
        assert self.addresses in self.metadata.sorted_tables

    def test_inserts(self):
        self.ins = self.users.insert()

        # inserts by default require all columns to be provided
        assert(str(self.ins) == 'INSERT INTO my_users (uid, name, fullname) VALUES (:uid, :name, :fullname)')

        # use the VALUES clause to limit the values inserted
        self.ins = self.users.insert().values(name='mark', fullname='mark sandan')

        # actual values don't get stored in the string
        assert(str(self.ins) == 'INSERT INTO my_users (name, fullname) VALUES (:name, :fullname)')

        # data values are stored in the INSERT but only are used when executed, we can peek
        assert(str(self.ins.compile().params) == str({'name': 'mark', 'fullname': 'mark sandan'}))

        # None of the inserts above in this test get added to the database

    def test_executing(self):
        # re-create a new INSERT object
        self.ins = self.users.insert()

        # execute the insert statement
        res = self.conn.execute(self.ins, uid=1, name='jack', fullname='Jack Jones')
        assert(res.inserted_primary_key == [1])
        res = self.conn.execute(self.ins, uid=2, name='wendy', fullname='Wendy Williams')
        assert(res.inserted_primary_key == [2])

        # the res variable is a ResultProxy object, analagous to DBAPI cursor

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
        assert(u1['name'] == u'jack')
        assert(u1['fullname'] == u'Jack Jones')

        assert(u2['name'] == u'wendy')
        assert(u2['fullname'] == u'Wendy Williams')

        assert(u1[1] == u1['name'])
        assert(u1[2] == u1['fullname'])

        assert(u2[1] == u2['name'])
        assert(u2[2] == u2['fullname'])

        # be sure to close the result set
        res.close()

        # use cols to access rows
        res = self.conn.execute(s)
        u3 = res.fetchone()
        u4 = res.fetchone()

        assert(u3[self.users.c.name] == u1['name'])
        assert(u3[self.users.c.fullname] == u1['fullname'])

        assert(u4[self.users.c.name] == u2['name'])
        assert(u4[self.users.c.fullname] == u2['fullname'])

        # reference individual columns in select clause
        s = select([self.users.c.name, self.users.c.fullname])
        res = self.conn.execute(s)
        u3 = res.fetchone()
        u4 = res.fetchone()

        assert(u3[self.users.c.name] == u1['name'])
        assert(u3[self.users.c.fullname] == u1['fullname'])

        assert(u4[self.users.c.name] == u2['name'])
        assert(u4[self.users.c.fullname] == u2['fullname'])

        # test joins
        # cartesian product
        usrs = [row for row in self.conn.execute(select([self.users]))]
        addrs = [row for row in self.conn.execute(select([self.addresses]))]
        prod = [row for row in self.conn.execute(select([self.users, self.addresses]))]
        assert(len(prod) == len(usrs) * len(addrs))

        # inner join on id
        s = select([self.users, self.addresses]).where(self.users.c.uid == self.addresses.c.user_id)
        inner = [row for row in self.conn.execute(s)]
        assert(len(inner) == 4)

        # operators between columns objects & other col objects/literals
        expr = self.users.c.uid == self.addresses.c.user_id
        assert('my_users.uid = addresses.user_id' == str(expr))
        # see how Teradata concats two strings
        assert(str((self.users.c.name + self.users.c.fullname).compile(bind=self.engine)) ==
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
        # print(s)
        res = self.conn.execute(s)
        for row in res:
            assert(str(row[0]) == u'Wendy Williams, wendy@aol.com')

        # more joins
        # ON condition auto generated based on ForeignKey
        assert(str(self.users.join(self.addresses)) ==
               'my_users JOIN addresses ON my_users.uid = addresses.user_id')

        # specify the join ON condition
        self.users.join(self.addresses,
                        self.addresses.c.email_address.like(self.users.c.name + '%'))

        # select from clause to specify tables and the ON condition
        s = select([self.users.c.fullname]).select_from(
            self.users.join(self.addresses, self.addresses.c.email_address.like(self.users.c.name + '%')))
        res = self.conn.execute(s)
        assert(len(res.fetchall()) == 3)

        # left outer joins
        s = select([self.users.c.fullname]).select_from(self.users.outerjoin(self.addresses))
        # outer join works with teradata dialect (unlike oracle dialect < version9)

        assert(str(s) == str(s.compile(dialect=self.dialect)))

        # test bind params (positional)

        from sqlalchemy import text
        s = self.users.select(self.users.c.name.like(
                                bindparam('username', type_=String)+text("'%'")))
        res = self.conn.execute(s, username='wendy').fetchall()
        assert(len(res) == 1)

        # functions
        from sqlalchemy.sql import func, column

        # certain function names are known by sqlalchemy
        assert(str(func.current_timestamp()) == 'CURRENT_TIMESTAMP')

        # functions can be used in the select
        res = self.conn.execute(select(
            [func.max(self.addresses.c.email_address, type_=String).label(
                'max_email')])).scalar()
        assert(res == 'www@www.com')

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
        assert('SELECT my_users.uid, my_users.name, my_users.fullname \n'
               'FROM my_users, (SELECT q, z, r \n'
                               'FROM calculate(:x, :y)) AS anon_1 \n'
               'WHERE my_users.uid > anon_1.z' == str(s))
        # instantiate the func
        calc1 = calculate.alias('c1').unique_params(x=17, y=45)
        calc2 = calculate.alias('c2').unique_params(x=5, y=12)

        s = select([self.users]).where(self.users.c.uid.between(calc1.c.z, calc2.c.z))
        parms = s.compile().params

        assert('x_2' in parms)
        assert('x_1' in parms)
        assert('y_2' in parms)
        assert('y_1' in parms)
        assert(parms['x_1'] == 17)
        assert(parms['y_1'] == 45)
        assert(parms['x_2'] == 5)
        assert(parms['y_2'] == 12)

        # order by asc
        stmt = select([self.users.c.name]).order_by(self.users.c.name)
        res = self.conn.execute(stmt).fetchall()

        assert('jack' == res[0][0])
        assert('wendy' == res[1][0])

        # order by desc
        stmt = select([self.users.c.name]).order_by(self.users.c.name.desc())
        res = self.conn.execute(stmt).fetchall()

        assert('wendy' == res[0][0])
        assert('jack' == res[1][0])

        # group by
        stmt = select([self.users.c.name, func.count(self.addresses.c.id)]).\
            select_from(self.users.join(self.addresses)).\
            group_by(self.users.c.name)

        res = self.conn.execute(stmt).fetchall()

        # TODO order by?
        # assert(res[0][0] == 'wendy')
        # assert(res[1][0] == 'jack')
        assert(res[0][1] == res[1][1])

        # group by having
        stmt = select([self.users.c.name, func.count(self.addresses.c.id)]).\
            select_from(self.users.join(self.addresses)).\
            group_by(self.users.c.name).\
            having(func.length(self.users.c.name) > 4)

        res = self.conn.execute(stmt).fetchall()

        assert(res[0] == ('wendy', 2))

        # distinct
        stmt = select([self.users.c.name]).\
            where(self.addresses.c.email_address.contains(self.users.c.name)).distinct()

        res = self.conn.execute(stmt).fetchall()

        assert(len(res) == 2)
        assert(res[0][0] != res[1][0])

        # limit
        stmt = select([self.users.c.name, self.addresses.c.email_address]).\
            select_from(self.users.join(self.addresses)).\
            limit(1)

        res = self.conn.execute(stmt).fetchall()

        assert(len(res) == 1)

        # offset

        # test union and except
        from sqlalchemy.sql import except_, union

        u = union(
            self.addresses.select().where(self.addresses.c.email_address == 'foo@bar.com'),
            self.addresses.select().where(self.addresses.c.email_address.like('%@yahoo.com')),)# .order_by(self.addresses.c.email_address)
        # print(u)
        # #res = self.conn.execute(u) this fails, syntax error order by expects pos integer?

        u = except_(
              self.addresses.select().where(self.addresses.c.email_address.like('%@%.com')),
              self.addresses.select().where(self.addresses.c.email_address.like('%@msn.com')))
        res = self.conn.execute(u).fetchall()
        # TODO Should be 3?
        # assert(1 == len(res))

        u = except_(
               union(
                  self.addresses.select().where(self.addresses.c.email_address.like('%@yahoo.com')),
                  self.addresses.select().where(self.addresses.c.email_address.like('%@msn.com'))
               ).alias().select(), self.addresses.select(self.addresses.c.email_address.like('%@msn.com'))
        )

        res = self.conn.execute(u).fetchall()
        assert(1 == len(res))

        # scalar subqueries
        stmt = select([func.count(self.addresses.c.id)]).where(self.users.c.uid == self.addresses.c.user_id).as_scalar()

        # we can place stmt as any other column within another select
        res = self.conn.execute(select([self.users.c.name, stmt])).fetchall()

        # res is a list of tuples, one tuple per user's name
        assert(2 == len(res))

        u1 = res[0]
        u2 = res[1]

        assert(len(u1) == len(u2))
        assert(u1[0] == u'jack')
        assert(u1[1] == u2[1])
        assert(u2[0] == u'wendy')

        # we can label the inner query
        stmt = select([func.count(self.addresses.c.id)]).\
            where(self.users.c.uid == self.addresses.c.user_id).\
            label("address_count")

        res = self.conn.execute(select([self.users.c.name, stmt])).fetchall()
        assert(2 == len(res))

        u1 = res[0]
        u2 = res[1]

        assert(len(u1) == 2)
        assert(len(u2) == 2)

        # inserts, updates, deletes
        stmt = self.users.update().values(fullname="Fullname: " + self.users.c.name)
        res = self.conn.execute(stmt)

        assert('name_1' in res.last_updated_params())
        assert(res.last_updated_params()['name_1'] == 'Fullname: ')

        stmt = self.users.insert().values(name=bindparam('_name') + " .. name")
        res = self.conn.execute(stmt, [{'uid': 4, '_name': 'name1'}, {'uid': 5, '_name': 'name2'}, {'uid': 6, '_name': 'name3'}, ])

        # updates
        stmt = self.users.update().where(self.users.c.name == 'jack').values(name='ed')
        res = self.conn.execute(stmt)

        assert(res.rowcount == 1)
        assert(res.returns_rows is False)

        # update many with bound params
        stmt = self.users.update().where(self.users.c.name == bindparam('oldname')).\
            values(name=bindparam('newname'))
        res = self.conn.execute(stmt, [
                   {'oldname': 'jack', 'newname': 'ed'},
                   {'oldname': 'wendy', 'newname': 'mary'},
        ])

        assert(res.returns_rows is False)
        assert(res.rowcount == 1)

        res = self.conn.execute(select([self.users]).where(self.users.c.name == 'ed'))
        r = res.fetchone()
        assert(r['name'] == 'ed')

        # correlated updates
        stmt = select([self.addresses.c.email_address]).\
            where(self.addresses.c.user_id == self.users.c.uid).\
            limit(1)
        # this fails, syntax error bc of LIMIT - need TOP/SAMPLE instead
        # Note: TOP can't be in a subquery
        # res = self.conn.execute(self.users.update().values(fullname=stmt))

        # multiple table updates
        stmt = self.users.update().\
            values(name='ed wood').\
            where(self.users.c.uid == self.addresses.c.id).\
            where(self.addresses.c.email_address.startswith('ed%'))

        # this fails, teradata does update from set where not update set from where
        # #res = self.conn.execute(stmt)

        stmt = self.users.update().\
            values({
               self.users.c.name: 'ed wood',
               self.addresses.c.email_address: 'ed.wood@foo.com'
            }).\
            where(self.users.c.uid == self.addresses.c.id).\
            where(self.addresses.c.email_address.startswith('ed%'))

        # fails but works on MySQL, should this work for us?
        # #res = self.conn.execute(stmt)

        # deletes
        self.conn.execute(self.addresses.delete())
        self.conn.execute(self.users.delete().where(self.users.c.name > 'm'))

        # matched row counts
        # updates + deletes have a number indicating # rows matched by WHERE clause
        res = self.conn.execute(self.users.delete())
        assert(res.rowcount == 1)
