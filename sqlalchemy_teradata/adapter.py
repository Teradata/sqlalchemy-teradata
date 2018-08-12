from sqlalchemy import util
from sqlalchemy_teradata import types
from sqlalchemy.sql import operators


class TeradataExpressionAdapter:
    """Expression Adapter for Teradata Data Types.

    For inferring the resulting type of a BinaryExpression whose operation
    involves operands that are of Teradata types.
    """

    def process(self, type_, op=None, other=None, **kw):
        """Adapts the expression.

        Infer the type of the resultant BinaryExpression defined by the passed
        in operator and operands

        Args:
            type_: The type of the left operand.

            op:    The operator of the BinaryExpression.

            other: The type of the right operand.

        Returns:
            The type to adapt the BinaryExpression to.
        """

        return getattr(self, 'visit_' + type_.__visit_name__)(type_, **kw) \
            .get(op, util.immutabledict()) \
            .get(other, type_)

    @staticmethod
    def _flatten_tuple_dict(tuple_dict):
        """Flatten a dictionary with (many-to-one) tuple keys to a
        standard one."""

        flat_dict = {}
        for k_tup, v in tuple_dict.items():
            for k in k_tup:
                flat_dict[k] = v
        return flat_dict

    def visit_INTEGER(self, type_, **kw):
        return self._flatten_tuple_dict({
            (operators.add, operators.sub, operators.mul,
             operators.truediv, operators.mod): {
                types.BIGINT:  types.BIGINT,
                types.DECIMAL: types.DECIMAL,
                types.FLOAT:   types.FLOAT,
                types.NUMBER:  types.NUMBER
            }
        })

    def visit_SMALLINT(self, type_, **kw):
        return self._flatten_tuple_dict({
            (operators.add, operators.sub, operators.mul,
             operators.truediv, operators.mod): {
                types.INTEGER:  types.INTEGER,
                types.SMALLINT: types.INTEGER,
                types.BIGINT:   types.BIGINT,
                types.DECIMAL:  types.DECIMAL,
                types.FLOAT:    types.FLOAT,
                types.NUMBER:   types.NUMBER,
                types.BYTEINT:  types.INTEGER
            }
        })

    def visit_BIGINT(self, type_, **kw):
        return self._flatten_tuple_dict({
            (operators.add, operators.sub, operators.mul,
             operators.truediv, operators.mod): {
                types.DECIMAL: types.DECIMAL,
                types.FLOAT:   types.FLOAT,
                types.NUMBER:  types.NUMBER
            }
        })

    def visit_DECIMAL(self, type_, **kw):
        return self._flatten_tuple_dict({
            (operators.add, operators.sub, operators.mul,
             operators.truediv, operators.mod): {
                types.FLOAT:  types.FLOAT,
                types.NUMBER: types.NUMBER
            }
        })

    def visit_DATE(self, type_, **kw):
        return self._flatten_tuple_dict({
            (operators.add, operators.sub, operators.mul,
             operators.truediv, operators.mod): {
                types.DATE: types.INTEGER
            }
        })

    def visit_INTERVAL_YEAR(self, type_, **kw):
        return {}

    def visit_INTERVAL_YEAR_TO_MONTH(self, type_, **kw):
        return {}

    def visit_INTERVAL_MONTH(self, type_, **kw):
        return {}

    def visit_INTERVAL_DAY(self, type_, **kw):
        return {}

    def visit_INTERVAL_DAY_TO_HOUR(self, type_, **kw):
        return {}

    def visit_INTERVAL_DAY_TO_MINUTE(self, type_, **kw):
        return {}

    def visit_INTERVAL_DAY_TO_SECOND(self, type_, **kw):
        return {}

    def visit_INTERVAL_HOUR(self, type_, **kw):
        return {}

    def visit_INTERVAL_HOUR_TO_MINUTE(self, type_, **kw):
        return {}

    def visit_INTERVAL_HOUR_TO_SECOND(self, type_, **kw):
        return {}

    def visit_INTERVAL_MINUTE(self, type_, **kw):
        return {}

    def visit_INTERVAL_MINUTE_TO_SECOND(self, type_, **kw):
        return {}

    def visit_INTERVAL_SECOND(self, type_, **kw):
        return {}

    def visit_PERIOD_DATE(self, type_, **kw):
        return {}

    def visit_PERIOD_TIME(self, type_, **kw):
        return {}

    def visit_PERIOD_TIMESTAMP(self, type_, **kw):
        return {}

    def visit_TIME(self, type_, **kw):
        return {}

    def visit_TIMESTAMP(self, type_, **kw):
        return {}

    def visit_CHAR(self, type_, **kw):
        return {
            operators.concat_op: {
                types.VARCHAR: types.VARCHAR,
                types.CLOB:    types.CLOB
            }
        }

    def visit_VARCHAR(self, type_, **kw):
        return {
            operators.concat_op: {
                types.CLOB: types.CLOB
            }
        }

    def visit_CLOB(self, type_, **kw):
        return {}

    def visit_BYTEINT(self, type_, **kw):
        return self._flatten_tuple_dict({
            (operators.add, operators.sub, operators.mul,
             operators.truediv, operators.mod): {
                types.INTEGER:  types.INTEGER,
                types.SMALLINT: types.INTEGER,
                types.BIGINT:   types.BIGINT,
                types.DECIMAL:  types.DECIMAL,
                types.FLOAT:    types.FLOAT,
                types.NUMBER:   types.NUMBER,
                types.BYTEINT:  types.INTEGER
            }
        })
    def visit_FLOAT(self, type_, **kw):
        return {}

    def visit_BYTE(self, type_, **kw):
        return {
            operators.concat_op: {
                types.VARBYTE: types.VARBYTE,
                types.BLOB:    types.BLOB
            }
        }

    def visit_VARBYTE(self, type_, **kw):
        return {
            operators.concat_op: {
                types.BLOB: types.BLOB
            }
        }

    def visit_BLOB(self, type_, **kw):
        return {}

    def visit_NUMBER(self, type_, **kw):
        return self._flatten_tuple_dict({
            (operators.add, operators.sub, operators.mul,
             operators.truediv, operators.mod): {
                types.FLOAT: types.FLOAT
            }
        })
