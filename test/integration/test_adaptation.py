from sqlalchemy import Table, Column, MetaData
from sqlalchemy import testing
from sqlalchemy import sql
from sqlalchemy.sql import operators
from sqlalchemy.testing.plugin.pytestplugin import *
from sqlalchemy_teradata.base import CreateView, DropView

import datetime, decimal
import itertools
import sqlalchemy_teradata as sqlalch_td
import teradata.datatypes as td_dtypes

"""
Integration testing for Teradata type adaptation.
"""

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
            Column('c2', sqlalch_td.CLOB(length=100)),
            Column('c3', sqlalch_td.CHAR(length=100, charset='unicode')),
            Column('c4', sqlalch_td.VARCHAR(length=100, charset='unicode')),
            Column('c5', sqlalch_td.CLOB(length=100, charset='unicode')))
        cls.table_datetime = Table('t_test_datetime', cls.metadata,
            Column('c0', sqlalch_td.DATE()),
            Column('c1', sqlalch_td.TIME()),
            Column('c2', sqlalch_td.TIMESTAMP()))
        cls.table_binary = Table('t_test_binary', cls.metadata,
            Column('c0', sqlalch_td.BYTE(length=100)),
            Column('c1', sqlalch_td.VARBYTE(length=100)),
            Column('c2', sqlalch_td.BLOB(length=100)))
        cls.table_interval= Table('t_test_interval', cls.metadata,
            Column('c0',  sqlalch_td.INTERVAL_YEAR()),
            Column('c1',  sqlalch_td.INTERVAL_YEAR_TO_MONTH()),
            Column('c2',  sqlalch_td.INTERVAL_MONTH()),
            Column('c3',  sqlalch_td.INTERVAL_DAY()),
            Column('c4',  sqlalch_td.INTERVAL_DAY_TO_HOUR()),
            Column('c5',  sqlalch_td.INTERVAL_DAY_TO_MINUTE()),
            Column('c6',  sqlalch_td.INTERVAL_DAY_TO_SECOND()),
            Column('c7',  sqlalch_td.INTERVAL_HOUR()),
            Column('c8',  sqlalch_td.INTERVAL_HOUR_TO_MINUTE()),
            Column('c9',  sqlalch_td.INTERVAL_HOUR_TO_SECOND()),
            Column('c10', sqlalch_td.INTERVAL_MINUTE()),
            Column('c11', sqlalch_td.INTERVAL_MINUTE_TO_SECOND()),
            Column('c12', sqlalch_td.INTERVAL_SECOND()))
        cls.metadata.create_all()

    def teardown_class(cls):
        cls.metadata.drop_all(cls.engine)
        cls.conn.close()

    def setup(self):
        self.conn     = testing.db.connect()
        self.engine   = self.conn.engine
        self.metadata = MetaData(bind=self.engine)

    def tearDown(self):
        self._drop_view('view_test')
        self.conn.invalidate()
        self.conn.close()

    def _drop_view(self, view_name):
        """Drop the specified view from the database."""

        view_names = self.engine.dialect.get_view_names(self.conn)
        if view_name in view_names:
            self.engine.execute(DropView(view_name))

    @staticmethod
    def _generate_intraclass_triples(cols, ops):
        """Enumerate all possible intraclass operand-operator triples."""

        triples = []
        for left_operand in cols:
            for right_operand in cols:
                for op in ops:
                    triples.append(op(left_operand, right_operand))
        return triples

    @staticmethod
    def _generate_interclass_triples(classes_tuple, ops):
        """Enumerate all possible interclass operand-operator triples."""

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
                res = self.engine.execute(sql.select([triple]).compile(
                    bind=self.engine, compile_kwargs={'literal_binds': True}))
                res.close()
                valid_triples.append(triple)
            except Exception:
                pass
        triples = valid_triples

        # Generate view with the valid operand-operator triples
        labeled_triples = [triple.label('c' + str(i)) for i, triple in enumerate(triples)]
        self.engine.execute(CreateView('view_test', sql.select(labeled_triples)))

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
        interval  = self._generate_intraclass_triples(self.table_interval.c, ops)
        triples   = numeric + character + datetime + binary + interval

        self._test_adaptation(triples)

    def test_interclass_arithmetic_adaptation(self):
        """Test arithmetic adaptations of types across different type classes."""

        ops           = self.arith_ops
        classes_tuple = (self.table_numeric.c, self.table_character.c,
                         self.table_datetime.c, self.table_binary.c,
                         self.table_interval.c)

        triples = self._generate_interclass_triples(classes_tuple, ops)

        self._test_adaptation(triples)

    def test_literal_arithmetic_adaptation(self):
        """Test arithmetic adaptations between Python literals and Teradata
        types.
        """

        ops           = self.arith_ops
        literals      = (10, 3.1415926535897932383,
                         decimal.Decimal(11), decimal.Decimal('11.1'),
                         str.encode('foobar'),
                         'foobar', u'foob\u00e3r',
                         datetime.date(year=1, month=2, day=3),
                         datetime.time(hour=15, minute=37, second=25),
                         datetime.datetime(year=1912, month=6, day=23,
                                           hour=15, minute=37, second=25),
                         td_dtypes.Interval(years=20),
                         td_dtypes.Interval(years=20, months=20),
                         td_dtypes.Interval(months=20),
                         td_dtypes.Interval(days=20),
                         td_dtypes.Interval(days=20, hours=20),
                         td_dtypes.Interval(days=20, minutes=20),
                         td_dtypes.Interval(days=20, seconds=20.20),
                         td_dtypes.Interval(hours=20),
                         td_dtypes.Interval(hours=20, minutes=20),
                         td_dtypes.Interval(hours=20, seconds=20.20),
                         td_dtypes.Interval(minutes=20),
                         td_dtypes.Interval(minutes=20, seconds=20.20),
                         td_dtypes.Interval(seconds=20.20))
        classes_tuple = (tuple(self.table_numeric.c + self.table_character.c +\
                               self.table_datetime.c + self.table_binary.c +\
                               self.table_interval.c),
                         literals)

        triples = self._generate_interclass_triples(classes_tuple, ops)

        self._test_adaptation(triples)
