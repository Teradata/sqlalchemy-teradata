from sqlalchemy import Table, Column, Index, text
from sqlalchemy import MetaData, create_engine
from sqlalchemy import sql
from sqlalchemy.sql import operators
from sqlalchemy.testing import fixtures
from sqlalchemy.testing.plugin.pytestplugin import *
from sqlalchemy_teradata.dialect import TeradataDialect

import itertools
import sqlalchemy_teradata as sqlalch_td

"""
Unit testing for SQLAlchemy Operators.
"""

class TestCompileOperators(fixtures.TestBase):

    def setup(self):
        def dump(sql, *multiparams, **params):
            self.last_compiled = str(sql.compile(dialect=self.td_engine.dialect))
        self.td_engine = create_engine('teradata://', strategy='mock', executor=dump)
        self.metadata  = MetaData(bind=self.td_engine)

        self.table = Table('t_test', self.metadata, Column('c1', sqlalch_td.INTEGER))

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


class TestCompileAffinity(fixtures.TestBase):

    def setup_class(cls):
        """
        Creates test tables to be used for testing type-operator affinities.
        """
        cls.metadata = MetaData()

        cls.table_numeric = Table('t_test_numeric', cls.metadata,
            Column('c0', sqlalch_td.INTEGER()),
            Column('c1', sqlalch_td.SMALLINT()),
            Column('c2', sqlalch_td.BIGINT()),
            Column('c3', sqlalch_td.DECIMAL()),
            Column('c4', sqlalch_td.FLOAT()),
            Column('c5', sqlalch_td.NUMBER()),
            Column('c6', sqlalch_td.BYTEINT()))
        cls.table_character = Table('t_test_character', cls.metadata,
            Column('c0', sqlalch_td.CHAR()),
            Column('c1', sqlalch_td.VARCHAR()),
            Column('c2', sqlalch_td.CLOB()))
        cls.table_datetime = Table('t_test_datetime', cls.metadata,
            Column('c0', sqlalch_td.DATE()),
            Column('c1', sqlalch_td.TIME()),
            Column('c2', sqlalch_td.TIMESTAMP()))
        cls.table_binary = Table('t_test_binary', cls.metadata,
            Column('c0', sqlalch_td.BYTE()),
            Column('c1', sqlalch_td.VARBYTE(10)),
            Column('c2', sqlalch_td.BLOB()))

        cls.arith_ops = (operators.add, operators.sub, operators.mul,
                         operators.div, operators.truediv, operators.mod)

    @staticmethod
    def _generate_op_triples(cols, ops):
        triples = []
        for left_operand in cols:
            for right_operand in cols:
                for op in ops:
                    triples.append(op(left_operand, right_operand))
        return triples

    def test_compile_arithmetic_numeric(self):
        triples = self._generate_op_triples(self.table_numeric.c, self.arith_ops)

        op_map = {
            operators.add:     '+',
            operators.sub:     '-',
            operators.mul:     '*',
            operators.truediv: '/',
            operators.mod:     'MOD'
        }

        for triple in triples:
            assert(op_map[triple.operator] in
                str(triple.compile(dialect=TeradataDialect())))

    def test_compile_arithmetic_date(self):
        triples = self._generate_op_triples([self.table_datetime.c.c0],
            [operators.add, operators.sub, operators.div,
             operators.truediv, operators.mod])

        op_map = {
            operators.add:     '+',
            operators.sub:     '-',
            operators.truediv: '/',
            operators.mod:     'MOD'
        }

        for triple in triples:
            assert(op_map[triple.operator] in
                str(triple.compile(dialect=TeradataDialect())))

    def test_compile_concat_character(self):
        triples = self._generate_op_triples(self.table_character.c,
            [operators.add, operators.concat_op])

        for triple in triples:
            assert('||' in str(triple.compile(dialect=TeradataDialect())))

    def test_compile_concat_binary(self):
        triples = self._generate_op_triples(self.table_binary.c,
            [operators.add, operators.concat_op])

        for triple in triples:
            assert('||' in str(triple.compile(dialect=TeradataDialect())))
