from sqlalchemy import Table, Column, MetaData, text
from sqlalchemy import testing
from sqlalchemy import sql
from sqlalchemy.sql import operators, sqltypes
from sqlalchemy.testing.plugin.pytestplugin import *
from sqlalchemy_teradata.dialect import TeradataDialect

import datetime, decimal, enum
import itertools
import sqlalchemy_teradata as sqlalch_td

"""
Integration testing for SQLAlchemy Operators and Type Affinities.
"""

class TestOperators(testing.fixtures.TestBase):

    def setup_class(cls):
        """
        Creates a test table to be used for testing operators.
        """
        cls.conn     = testing.db.connect()
        cls.engine   = cls.conn.engine
        cls.metadata = MetaData(bind=cls.engine)

        cls.table = Table('t_test', cls.metadata,
            Column('c0', sqlalch_td.VARCHAR(100)),
            Column('c1', sqlalch_td.VARCHAR(100)),
            Column('c2', sqlalch_td.INTEGER()),
            Column('c3', sqlalch_td.INTEGER()))
        cls.metadata.create_all()
        cls.engine.execute(
            cls.table.insert(),
            {'c0': 'the',    'c1': 'quick',  'c2': 1,  'c3': 10},
            {'c0': 'quick',  'c1': 'brown',  'c2': 2,  'c3': 9},
            {'c0': 'brown',  'c1': 'fox',    'c2': 3,  'c3': 8},
            {'c0': 'fox',    'c1': 'jumps',  'c2': 4,  'c3': 7},
            {'c0': 'jumps',  'c1': 'over',   'c2': 5,  'c3': 6},
            {'c0': 'over',   'c1': 'the',    'c2': 6,  'c3': 5},
            {'c0': 'the',    'c1': 'lazy',   'c2': 7,  'c3': 4},
            {'c0': 'lazy',   'c1': 'dog',    'c2': 8,  'c3': 3},
            {'c0': 'dog',    'c1': 'foobar', 'c2': 9,  'c3': 2},
            {'c0': 'foobar', 'c1': None,     'c2': 10, 'c3': 1}
        )

    def teardown_class(cls):
        cls.metadata.drop_all(cls.engine)
        cls.conn.close()

    def setup(self):
        self.conn     = testing.db.connect()
        self.engine   = self.conn.engine
        self.metadata = MetaData(bind=self.engine)

    def tearDown(self):
        self.conn.invalidate()
        self.conn.close()

    def test_arithmetic_operators(self):
        """
        Tests the functionality of the following arithmetic operators:

            +, -, *, /, truediv, mod
        """
        ops = (operators.add, operators.sub, operators.mul, operators.div,
               operators.truediv, operators.mod)
        res = self.engine.execute(
            sql.select(
                [op(self.table.c.c2, self.table.c.c3) for op in ops]). \
            order_by(self.table.c.c2.asc()))

        expected = (
            (11, -9,  10,  0,  0,  1),
            (11, -7,  18,  0,  0,  2),
            (11, -5,  24,  0,  0,  3),
            (11, -3,  28,  0,  0,  4),
            (11, -1,  30,  0,  0,  5),
            (11,  1,  30,  1,  1,  1),
            (11,  3,  28,  1,  1,  3),
            (11,  5,  24,  2,  2,  2),
            (11,  7,  18,  4,  4,  1),
            (11,  9,  10, 10, 10,  0)
        )
        for act_row, exp_row in zip(res, expected):
            assert(tuple(int(cell) for cell in act_row) == exp_row)

    def test_negative_operator(self):
        """
        Tests the functionality of the negative (-) operator.
        """
        res = self.engine.execute(
            sql.select(
                [operators.neg(self.table.c.c2)]). \
            order_by(self.table.c.c2.asc()))

        assert(tuple(int(row[0]) for row in res) == tuple(range(-1, -11, -1)))

    def test_relational_operators(self):
        """
        Tests the functionality of the following relational operators:

            <, <=, !=, >, >=, ==
        """
        ops_exp = {
            operators.lt: {'the', 'quick', 'brown', 'fox'},
            operators.le: {'the', 'quick', 'brown', 'fox', 'jumps'},
            operators.ne: {'the', 'quick', 'brown', 'fox', 'over', 'the',
                           'lazy', 'dog', 'foobar'},
            operators.gt: {'over', 'the', 'lazy', 'dog', 'foobar'},
            operators.ge: {'jumps', 'over', 'the', 'lazy', 'dog', 'foobar'},
            operators.eq: {'jumps'}
        }

        for op, exp in ops_exp.items():
            res = self.engine.execute(
                sql.select([self.table.c.c0]). \
                where(op(self.table.c.c2, 5)))

            assert(set([row[0] for row in res]) == exp)

    def test_logical_operators(self):
        """
        Tests the functionality of the following logical operators:

            AND, OR
        """
        res = self.engine.execute(
            sql.select([self.table.c.c0]). \
            where(operators.and_(
                operators.gt(self.table.c.c2, 3),
                operators.lt(self.table.c.c2, 7))))

        assert(set([row[0] for row in res]) == {'fox', 'jumps', 'over'})

        res = self.engine.execute(
            sql.select([self.table.c.c0]). \
            where(operators.or_(
                operators.lt(self.table.c.c2, 3),
                operators.gt(self.table.c.c2, 7))))

        assert(set([row[0] for row in res]) == {'the', 'quick', 'lazy',
                                                'dog', 'foobar'})

    def test_string_operators(self):
        """
        Tests the functionality of the following string operators:

            ||, LIKE
        """
        res = self.engine.execute(
            sql.select([
                operators.concat_op(self.table.c.c0, self.table.c.c1)]))

        assert(set([row[0] for row in res]) ==
            {'thequick', 'quickbrown', 'brownfox', 'foxjumps',  'jumpsover',
             'overthe',  'thelazy',    'lazydog',  'dogfoobar', None})

        res = self.engine.execute(
            sql.select([self.table.c.c0]). \
            where(operators.like_op(self.table.c.c0, '%o%')))

        assert(set([row[0] for row in res]) == {'fox', 'dog', 'foobar',
                                                'over', 'brown'})

    def test_is_operators(self):
        """
        Tests the functionality of the IS (and NOT IS) operator(s):
        """
        ops_exp = {
            operators.is_:   {'foobar'},
            operators.isnot: {'the', 'quick', 'brown', 'fox', 'jumps', 'over',
                              'the', 'lazy', 'dog'}
        }

        for op, exp in ops_exp.items():
            res = self.engine.execute(
                sql.select([self.table.c.c0]). \
                where(op(self.table.c.c1, text('NULL'))))

            assert(set([row[0] for row in res]) == exp)

    def test_in_operators(self):
        """
        Tests the functionality of the IN (and NOT IN) operator(s):
        """
        ops_exp = {
            operators.in_op:    {2, 4, 6, 8, 10},
            operators.notin_op: {1, 3, 5, 7, 9}
        }

        for op, exp in ops_exp.items():
            res = self.engine.execute(
                sql.select([self.table.c.c2]). \
                where(op(self.table.c.c0,
                    ('quick', 'fox', 'over', 'lazy', 'foobar'))))

            assert(set([row[0] for row in res]) == exp)

    def test_unary_operators(self):
        """
        Tests the functionality of the following unary operators:

            NOT, DISTINCT
        """
        res = self.engine.execute(
            sql.select([self.table.c.c0]). \
            where(operators.inv(operators.ne(self.table.c.c2, 5))))

        assert(set([row[0] for row in res]) == {'jumps'})

        res = self.engine.execute(
            sql.select([operators.distinct_op(self.table.c.c0)]))
        assert(res.rowcount == 9)

    def test_any_all_operators(self):
        """
        Tests the functionality of the ANY and ALL operators.
        """
        ops_exp = {
            operators.any_op: {'quick', 'brown', 'fox', 'jumps', 'over', 'the',
                               'lazy', 'dog', 'foobar'},
            operators.all_op: {'foobar'}
        }

        for op, exp in ops_exp.items():
            res = self.engine.execute(
                sql.select([self.table.c.c0]). \
                where(operators.ge(
                    self.table.c.c2,
                    op(sql.select([self.table.c.c3]).as_scalar()))))

            assert(set([row[0] for row in res]) == exp)

    def test_modifier_operators(self):
        """
        Tests the functionality of the following modifier operators:

            ASC, DESC, NULLS FIRST, NULLS LAST
        """
        sentence = ('the', 'quick', 'brown', 'fox', 'jumps', 'over',
                    'the', 'lazy', 'dog', 'foobar')
        ops_exp = {
            operators.desc_op: sentence[::-1],
            operators.asc_op:  sentence,
        }

        for op, exp in ops_exp.items():
            res = self.engine.execute(
                sql.select([self.table.c.c0]). \
                order_by(op(self.table.c.c2)))

            assert(tuple(row[0] for row in res) == exp)

        res = self.engine.execute(
            sql.select([self.table.c.c0]). \
            order_by(operators.nullsfirst_op(self.table.c.c1)))

        # Check that the scalar from the first row is 'foobar'
        assert(res.fetchone()[0] == 'foobar')

        res = self.engine.execute(
            sql.select([self.table.c.c0]). \
            order_by(operators.nullslast_op(self.table.c.c1)))

        # Check that the scalar from the last row is 'foobar'
        assert([row[0] for row in res][-1] == 'foobar')


class TestAdaptation(testing.fixtures.TestBase):

    def setup_class(cls):
        """
        Create test tables to be used for testing type-operator affinities.
        """
        
        cls.conn     = testing.db.connect()
        cls.engine   = cls.conn.engine
        cls.metadata = MetaData(bind=cls.engine)

        cls.arith_ops = (operators.add, operators.sub, operators.mul,
                         operators.div, operators.truediv, operators.mod)

        class TestEnum(enum.Enum):
            one   = 1
            two   = 2
            three = 3

        cls.table_numeric = Table('t_test_numeric', cls.metadata,
            Column('c0', sqlalch_td.INTEGER()),
            Column('c1', sqlalch_td.SMALLINT()),
            Column('c2', sqlalch_td.BIGINT()),
            Column('c3', sqlalch_td.DECIMAL()),
            Column('c4', sqlalch_td.FLOAT()),
            Column('c5', sqlalch_td.NUMBER()),
            Column('c6', sqlalch_td.BYTEINT()))
        cls.table_character = Table('t_test_character', cls.metadata,
            Column('c0', sqlalch_td.CHAR(length=100)),
            Column('c1', sqlalch_td.VARCHAR(length=100)),
            Column('c2', sqlalch_td.CLOB(length=100)))
        cls.table_datetime = Table('t_test_datetime', cls.metadata,
            Column('c0', sqlalch_td.DATE()),
            Column('c1', sqlalch_td.TIME()),
            Column('c2', sqlalch_td.TIMESTAMP()))
        cls.table_binary = Table('t_test_binary', cls.metadata,
            Column('c0', sqlalch_td.BYTE(length=100)),
            Column('c1', sqlalch_td.VARBYTE(length=100)),
            Column('c2', sqlalch_td.BLOB(length=100)))
        cls.table_generic = Table('t_test_generic', cls.metadata,
            Column('c0',  sqltypes.BigInteger()),
            Column('c1',  sqltypes.Boolean()),
            Column('c2',  sqltypes.Date()),
            Column('c3',  sqltypes.DateTime()),
            Column('c4',  sqltypes.Enum(TestEnum)),
            Column('c5',  sqltypes.Float()),
            Column('c6',  sqltypes.Integer()),
            Column('c7',  sqltypes.Interval()),
            Column('c8',  sqltypes.LargeBinary()),
            Column('c9',  sqltypes.Numeric()),
            Column('c10', sqltypes.SmallInteger()),
            Column('c11', sqltypes.String()),
            Column('c12', sqltypes.Text()),
            Column('c13', sqltypes.Time()),
            Column('c14', sqltypes.Unicode()),
            Column('c15', sqltypes.UnicodeText()))
        cls.metadata.create_all()

        cls.engine.execute(cls.table_numeric.insert(),
            {'c0': 1, 'c1': 2, 'c2': 3, 'c3': 4,
             'c4': 5, 'c5': 6, 'c6': 7})
        cls.engine.execute(cls.table_character.insert(),
            {'c0': 'fizzbuzz', 'c1': 'foobar',
             'c2': 'the quick brown fox jumps over the lazy dog'})
        cls.engine.execute(cls.table_datetime.insert(),
            {'c0': datetime.date(year=3, month=2, day=1),
             'c1': datetime.time(hour=15, minute=37, second=25),
             'c2': datetime.datetime(year=1912, month=6, day=23,
                                     hour=15, minute=37, second=25)})
        cls.engine.execute(cls.table_binary.insert(),
            {'c0': str.encode('a'), 'c1': str.encode('b'),
             'c2': str.encode('c')})

    def teardown_class(cls):
        cls.metadata.drop_all(cls.engine)
        cls.conn.close()

    def setup(self):
        self.conn     = testing.db.connect()
        self.engine   = self.conn.engine
        self.metadata = MetaData(bind=self.engine)

    def tearDown(self):
        self._drop_view_test()

        self.conn.invalidate()
        self.conn.close()

    def _drop_view_test(self):
        """Drop view_test from the database."""

        view_names = self.engine.dialect.get_view_names(self.conn)
        for view_name in view_names:
            if view_name == 'view_test':
                self.conn.execute('DROP VIEW {}'.format(view_name))

    @staticmethod
    def _generate_intraclass_triples(cols, ops):
        """Enumerates all possible intraclass operand-operator triples."""

        triples = []
        for left_operand in cols:
            for right_operand in cols:
                for op in ops:
                    triples.append(op(left_operand, right_operand))
        return triples

    @staticmethod
    def _generate_interclass_triples(classes_tuple, ops):
        """Enumerates all possible interclass operand-operator triples."""

        triples = []
        for class_pair in itertools.permutations(classes_tuple, r=2):
            for type_pair in itertools.product(class_pair[0], class_pair[1]):
                for op in ops:
                    triples.append(op(type_pair[0], type_pair[1]))
        return triples

    def _test_adaptation(self, triples):
        """Check that each valid operand-operator triple adapts to the correct
        type class.

        For each operand-operator triple corresponding to a valid database
        operation, check that the BinaryExpression `type` field is equivalent to
        the type emitted by the Teradata backend.
        """

        # Filter for operand-operator triples that correspond to valid
        # operations on the database
        valid_triples = []
        for triple in triples:
            try:
                res = self.engine.execute(sql.select([triple]))
                res.close()
                valid_triples.append(triple)
            except Exception as exc:
                # TODO handle overflow with DATE * DATE and op(character, numeric)
                # if not 'operation' in str(exc):
                #     print(exc)
                pass
        triples = valid_triples

        # Generate view with the valid operand-operator triples
        c_ops = ', '.join([str(triple.compile(
                        dialect=TeradataDialect(),
                        compile_kwargs={"literal_binds": True})) +
                    ' as ' + 'c' + str(i)
                for i, triple in enumerate(triples)])
        create_view_stmt = \
            'CREATE VIEW view_test as (\nSELECT ' + c_ops + '\n)'
        self.engine.execute(text(create_view_stmt))

        # Reflect the view to check the type of each column
        view = Table('view_test', self.metadata, autoload=True)

        # Check that the type of the BinaryExpression is equal to the type
        # of the corresponding column in the view
        for i, triple in enumerate(triples):
            assert(type(triple.type) == type(view.columns['c' + str(i)].type))

    def test_intraclass_arithmetic_adaptation(self):
        """Test arithmetic adaptations of types within the same type class."""

        ops = self.arith_ops

        # Enumerate all possible intraclass operand-operator triples
        numeric   = self._generate_intraclass_triples(self.table_numeric.c, ops)
        character = self._generate_intraclass_triples(self.table_character.c, ops)
        datetime  = self._generate_intraclass_triples(self.table_datetime.c, ops)
        binary    = self._generate_intraclass_triples(self.table_binary.c, ops)
        triples = numeric + character + datetime + binary

        self._test_adaptation(triples)

    @pytest.mark.xfail
    def test_interclass_arithmetic_adaptation(self):
        """Test arithmetic adaptations of types across different type classes."""

        ops           = self.arith_ops
        classes_tuple = (self.table_numeric.c, self.table_character.c,
                         self.table_datetime.c, self.table_binary.c)

        triples = self._generate_interclass_triples(classes_tuple, ops)

        self._test_adaptation(triples)

    @pytest.mark.xfail
    def test_generic_arithmetic_adaptation(self):
        """Test arithmetic adaptations between generic and Teradata types."""

        ops           = self.arith_ops
        generics      = (self.table_generic.c)
        classes_tuple = ((*self.table_numeric.c, *self.table_character.c,
                          *self.table_datetime.c, *self.table_binary.c),
                         generics)

        triples = self._generate_interclass_triples(classes_tuple, ops)

        self._test_adaptation(triples)

    @pytest.mark.xfail
    def test_numeric_literal_arithmetic_adaptation(self):
        """Test arithmetic adaptations between numeric literals and
        Teradata types.
        """

        ops           = self.arith_ops
        literals      = (10, 10.0, decimal.Decimal(10))
        classes_tuple = (self.table_numeric.c, literals)

        triples = self._generate_interclass_triples(classes_tuple, ops)

        self._test_adaptation(triples)
