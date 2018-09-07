# sqlalchemy_teradata/types.py
# Copyright (C) 2015-2019 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import datetime
import decimal
import sys
import warnings

from sqlalchemy import util, types
from sqlalchemy.sql import sqltypes, operators
from teradata import datatypes as td_dtypes


class _TDComparable:
    """Teradata comparable data type."""

    class Comparator(types.TypeEngine.Comparator):
        """Comparator for expression adaptation.

        Use the TeradataExpressionAdapter to process the resulting types
        for binary operations over Teradata types.
        """

        def _adapt_expression(self, op, other_comparator):
            expr_type = TeradataExpressionAdapter().process(
                self.type, op=op, other=other_comparator.type)

            return op, expr_type

    comparator_factory = Comparator


class _TDConcatenable:
    """Teradata concatenable data type.

    This family of types currently encompasses the binary types
    (BYTE, VARBYTE, BLOB) and the character types (CHAR, VARCHAR, CLOB).
    """

    class Comparator(_TDComparable.Comparator):
        """Comparator for expression adaptation.

        Overloads the addition (+) operator over concatenable Teradata types
        to use concat_op. Note that this overloading only occurs between types
        within the same type_affinity.
        """

        def _adapt_expression(self, op, other_comparator):
            return super(_TDConcatenable.Comparator, self)._adapt_expression(
                operators.concat_op
                if (op is operators.add
                    and isinstance(other_comparator.type,
                                   self.type._type_affinity))
                else op,
                other_comparator)

    comparator_factory = Comparator


class _TDLiteralCoercer:
    """Mixin for literal type processing against Teradata data types."""

    def coerce_compared_value(self, op, value):
        type_ = type(value)

        if type_ == int:
            return INTEGER()
        if type_ == float:
            return FLOAT()
        if type_ == bytes:
            return BYTE()
        if type_ == str:
            return VARCHAR()
        if type_ == decimal.Decimal:
            return DECIMAL()
        if type_ == datetime.date:
            return DATE()
        if type_ == datetime.datetime:
            return TIMESTAMP()
        if type_ == datetime.time:
            return TIME()
        if type_ == td_dtypes.Interval:
            return getattr(sys.modules[__name__],
                           'INTERVAL_{}'.format(value.type.replace(' ', '_')),
                           sqltypes.NullType)()
        # TODO: PERIOD

        return sqltypes.NullType()


class _TDType(_TDLiteralCoercer, _TDComparable):
    """Teradata data type.

    Identifies a Teradata data type. Currently used to override __str__
    behavior such that the type will get printed without being compiled by the
    GenericTypeCompiler (which would otherwise result in an exception).
    """

    def _parse_name(self, name):
        return name.replace('_', ' ')

    def __str__(self):
        return self._parse_name(self.__class__.__name__)


class BYTEINT(_TDType, sqltypes.Integer):
    """Teradata BYTEINT data type.

    Represents a signed binary integer value in the range -128 to 127.
    """

    __visit_name__ = 'BYTEINT'

    def __init__(self, **kwargs):
        """Construct a BYTEINT Object."""

        super(BYTEINT, self).__init__(**kwargs)


class SMALLINT(_TDType, sqltypes.SMALLINT):
    """Teradata SMALLINT data type.

    Represents a signed binary integer value in the range -32768 to 32767.
    """

    def __init__(self, **kwargs):
        """Construct a SMALLINT Object."""

        super(SMALLINT, self).__init__(**kwargs)


class INTEGER(_TDType, sqltypes.INTEGER):
    """Teradata INTEGER data type.

    Represents a signed, binary integer value from -2,147,483,648 to
    2,147,483,647.
    """

    def __init__(self, **kwargs):
        """Construct an INTEGER Object."""

        super(INTEGER, self).__init__(**kwargs)


class BIGINT(_TDType, sqltypes.BIGINT):
    """Teradata BIGINT data type.

    Represents a signed, binary integer value from -9,223,372,036,854,775,808
    to 9,223,372,036,854,775,807.
    """

    def __init__(self, **kwargs):
        """Construct a BIGINT Object."""

        super(BIGINT, self).__init__(**kwargs)


class DECIMAL(_TDType, sqltypes.DECIMAL):
    """Teradata DECIMAL data type.

    Represents a decimal number of `precision` digits, with `scale` of those
    `precision` digits to the right of the decimal point.
    """

    def __init__(self, precision=38, scale=19, **kwargs):
        """Construct a DECIMAL Object.

        Args:
            precision (int): The precision (the maximum number of digits that
                can be stored). The range is from 1 through 38.
            scale (int): The scale (the number of fractional digits). The range
                is from 0 through `precision`.

        Note:
            When values are not specified for `precision`, `scale`, then the
            default is DECIMAL(5, 0). When a value is not specified for
            `scale`, then the default is DECIMAL(`precision`, 0).
        """

        super(DECIMAL, self).__init__(precision=precision, scale=scale,
                                      **kwargs)

    def literal_processor(self, dialect):
        def process(value):
            return str(value) + ('' if value.as_tuple()[2] < 0 else '.')

        return process


class FLOAT(_TDType, sqltypes.FLOAT):
    """Teradata FLOAT data type.

    Represent values in sign/magnitude form ranging from 2.226 x 10^-308 to
    1.797 x 10^308.
    """

    def __init__(self, **kwargs):
        """Construct a FLOAT object."""

        super(FLOAT, self).__init__(**kwargs)

    def literal_processor(self, dialect):
        def process(value):
            return 'CAST({} as FLOAT)'.format(value)

        return process


class NUMBER(_TDType, sqltypes.NUMERIC):
    """Teradata NUMBER data type.

    Represents a numeric value with optional precision and scale limitations.
    """

    __visit_name__ = 'NUMBER'

    def __init__(self, precision=None, scale=None, **kwargs):
        """Construct a NUMBER object.

        Args:
            precision (int): The precision (the maximum number of digits that
                can be stored). The range is from 1 through 38.
            scale (int): The scale. This indicates the maximum number of digits
                allowed to the right of the decimal point. If `precision` is
                specified, the range of `scale` is from 0 to `precision`.

        Note:
            Both parameters are optional. When both are left unspecified,
            defaults to NUMBER with the system limits for precision and scale.
        """

        super(NUMBER, self).__init__(precision=precision, scale=scale, **kwargs)


class DATE(_TDType, sqltypes.DATE):
    """Teradata DATE data type.

    Identifies a field as a DATE value and simplifies handling and formatting
    of date variables.
    """

    def __init__(self, **kwargs):
        """Construct a DATE Object."""

        super(DATE, self).__init__(**kwargs)

    def literal_processor(self, dialect):
        def process(value):
            return "DATE '{}'".format(value)

        return process


class TIME(_TDType, sqltypes.TIME):
    """Teradata TIME data type.

    Identifies a field as a TIME value.
    """

    def __init__(self, precision=6, timezone=False, **kwargs):
        """Construct a TIME object.

        Args:
            precision (int): Optional fractional seconds precision. A single
                digit representing the number of significant digits in the
                fractional portion of the SECOND field. Values range from
                0 to 6 inclusive. The default precision is 6.
            timezone (bool): If set to True creates a TIME WITH TIME ZONE type.

        Note:
            TIME is stored as UTC in Teradata.
        """

        super(TIME, self).__init__(timezone=timezone, **kwargs)
        self.precision = precision

    def literal_processor(self, dialect):
        def process(value):
            return "TIME '{}'".format(value)

        return process


class TIMESTAMP(_TDType, sqltypes.TIMESTAMP):
    """Teradata TIMESTAMP data type.

    Identifies a field as a TIMESTAMP value.
    """

    def __init__(self, precision=6, timezone=False, **kwargs):
        """Construct a TIMESTAMP object.

        Args:
            precision (int): Optional fractional seconds precision. A single
                digit representing the number of significant digits in the
                fractional portion of the SECOND field. Values range from
                0 to 6 inclusive. The default precision is 6.
            timezone (bool): If set to True creates a TIMESTAMP WITH TIME ZONE
                type.

        Note:
            TIMESTAMP is stored as UTC in Teradata.
        """

        super(TIMESTAMP, self).__init__(timezone=timezone, **kwargs)
        self.precision = precision

    def literal_processor(self, dialect):
        def process(value):
            return "TIMESTAMP '{}'".format(value)

        return process

    def get_dbapi_type(self, dbapi):
        return dbapi.DATETIME


class _TDInterval(_TDType, types.UserDefinedType):
    """Base class for the Teradata INTERVAL data types."""

    def __init__(self, precision=None, frac_precision=None, **kwargs):
        """Construct an INTERVAL object.

        Args:
            precision (int): The permitted range of digits for YEAR, MONTH, DAY,
                HOUR, MINUTE, or SECOND. Values range from 1 to 4 inclusive.
                The default precision is 2.
            frac_preicision (int): The fractional precision for the values of
                SECOND, ranging from 0 to 6 (when applicable). The default
                fractional precision is 6.
        """

        self.precision = precision
        self.frac_precision = frac_precision

    def literal_processor(self, dialect):
        def process(value):
            return "INTERVAL '{}' {}".format(value, value.type)

        return process


class INTERVAL_YEAR(_TDInterval):
    """Teradata INTERVAL YEAR data type.

    Identifies a field as an INTERVAL value defining a period of time in years.
    """

    __visit_name__ = 'INTERVAL_YEAR'

    def __init__(self, precision=None, **kwargs):
        """Construct an INTERVAL_YEAR object.

        Args:
            precision (int): The permitted range of digits for YEAR, ranging
                from 1 to 4. The default precision is 2.
        """

        super(INTERVAL_YEAR, self).__init__(precision=precision)


class INTERVAL_YEAR_TO_MONTH(_TDInterval):
    """Teradata INTERVAL YEAR TO MONTH data type.

    Identifies a field as an INTERVAL value defining a period of time in years
    and months.
    """

    __visit_name__ = 'INTERVAL_YEAR_TO_MONTH'

    def __init__(self, precision=None, **kwargs):
        """Construct an INTERVAL_YEAR_TO_MONTH object.

        Args:
            precision (int): The permitted range of digits for YEAR, ranging
                from 1 to 4. The default precision is 2.
        """

        super(INTERVAL_YEAR_TO_MONTH, self).__init__(precision=precision)


class INTERVAL_MONTH(_TDInterval):
    """Teradata INTERVAL MONTH data type.

    Identifies a field as an INTERVAL value defining a period of time in months.
    """

    __visit_name__ = 'INTERVAL_MONTH'

    def __init__(self, precision=None, **kwargs):
        """Construct an INTERVAL_MONTH object.

        Args:
            precision (int): The permitted range of digits for MONTH, ranging
                from 1 to 4. The default precision is 2.
        """

        super(INTERVAL_MONTH, self).__init__(precision=precision)


class INTERVAL_DAY(_TDInterval):
    """Teradata INTERVAL DAY data type

    Identifies a field as an INTERVAL value defining a period of time in days.
    """

    __visit_name__ = 'INTERVAL_DAY'

    def __init__(self, precision=None, **kwargs):
        """Construct an INTERVAL_DAY object.

        Args:
            precision (int): The permitted range of digits for DAY, ranging
                from 1 to 4. The default precision is 2.
        """

        super(INTERVAL_DAY, self).__init__(precision=precision)

    def bind_processor(self, dialect):
        """Handles the processing of datetime.timedelta objects for inserting
        into columns of type INTERVAL DAY.

        Timedelta objects are converted to Teradata Interval objects
        appropriate for inserting into INTERVAL DAY columns.
        """

        def process(value):
            if isinstance(value, datetime.timedelta):
                value = td_dtypes.Interval(days=value.days)
            return value

        return process


class INTERVAL_DAY_TO_HOUR(_TDInterval):
    """Teradata INTERVAL DAY TO HOUR data type.

    Identifies a field as an INTERVAL value defining a period of time in days
    and hours.
    """

    __visit_name__ = 'INTERVAL_DAY_TO_HOUR'

    def __init__(self, precision=None, **kwargs):
        """Construct an INTERVAL_DAY_TO_HOUR object.

        Args:
            precision (int): The permitted range of digits for DAY, ranging
                from 1 to 4. The default precision is 2.
        """

        super(INTERVAL_DAY_TO_HOUR, self).__init__(precision=precision)

    def bind_processor(self, dialect):
        """Handles the processing of datetime.timedelta objects for inserting
        into columns of type INTERVAL DAY TO HOUR.

        Timedelta objects are converted to Teradata Interval objects
        appropriate for inserting into INTERVAL DAY TO HOUR columns.
        """

        def process(value):
            if isinstance(value, datetime.timedelta):
                hours = int(value.seconds / 3600)
                value = td_dtypes.Interval(days=value.days, hours=hours)
            return value

        return process


class INTERVAL_DAY_TO_MINUTE(_TDInterval):
    """Teradata INTERVAL DAY TO MINUTE data type.

    Identifies a field as an INTERVAL value defining a period of time in days,
    hours, and minutes.
    """

    __visit_name__ = 'INTERVAL_DAY_TO_MINUTE'

    def __init__(self, precision=None, **kwargs):
        """Construct an INTERVAL_DAY_TO_MINUTE object.

        Args:
            precision (int): The permitted range of digits for DAY, ranging
                from 1 to 4. The default precision is 2.
        """

        super(INTERVAL_DAY_TO_MINUTE, self).__init__(precision=precision)

    def bind_processor(self, dialect):
        """Handles the processing of datetime.timedelta objects for inserting
        into columns of type INTERVAL DAY TO MINUTE.

        Timedelta objects are converted to Teradata Interval objects
        appropriate for inserting into INTERVAL DAY TO MINUTE columns.
        """

        def process(value):
            if isinstance(value, datetime.timedelta):
                minutes = int(value.seconds / 60)
                value = td_dtypes.Interval(days=value.days, minutes=minutes)
            return value

        return process


class INTERVAL_DAY_TO_SECOND(_TDInterval):
    """Teradata INTERVAL DAY TO SECOND data type.

    Identifies a field as an INTERVAL value defining a period of time in days,
    hours, minutes, and seconds.
    """

    __visit_name__ = 'INTERVAL_DAY_TO_SECOND'

    def __init__(self, precision=None, frac_precision=None, **kwargs):
        """Construct an INTERVAL_DAY_TO_SECOND object.

        Args:
            precision (int): The permitted range of digits for DAY, ranging
                from 1 to 4. The default precision is 2.
            frac_precision (int): The fractional precision for the values of
                SECOND, ranging from 0 to 6. The default fractional precision
                is 6.
        """

        super(INTERVAL_DAY_TO_SECOND, self).__init__(
            precision=precision, frac_precision=frac_precision)

    def bind_processor(self, dialect):
        """Handles the processing of datetime.timedelta objects for inserting
        into columns of type INTERVAL DAY TO SECOND.

        Timedelta objects are converted to Teradata Interval objects
        appropriate for inserting into INTERVAL DAY TO SECOND columns.
        """

        def process(value):
            if isinstance(value, datetime.timedelta):
                seconds = value.seconds + value.microseconds / 1000000
                value = td_dtypes.Interval(days=value.days, seconds=seconds)
            return value

        return process


class INTERVAL_HOUR(_TDInterval):
    """Teradata INTERVAL HOUR data type.

    Identifies a field as an INTERVAL value defining a period of time in hours.
    """

    __visit_name__ = 'INTERVAL_HOUR'

    def __init__(self, precision=None, **kwargs):
        """Construct an INTERVAL_HOUR object.

        Args:
            precision (int): The ermitted range of digits for HOUR, ranging
                from 1 to 4. The default precision is 2.
        """

        super(INTERVAL_HOUR, self).__init__(precision=precision)

    def bind_processor(self, dialect):
        """Handles the processing of datetime.timedelta objects for inserting
        into columns of type INTERVAL HOUR.

        Timedelta objects are converted to Teradata Interval objects
        appropriate for inserting into INTERVAL HOUR columns.
        """

        def process(value):
            if isinstance(value, datetime.timedelta):
                hours = int(value.total_seconds() / 3600)
                value = td_dtypes.Interval(hours=hours)
            return value

        return process


class INTERVAL_HOUR_TO_MINUTE(_TDInterval):
    """Teradata INTERVAL HOUR TO MINUTE data type.

    Identifies a field as an INTERVAL value defining a period of time in hours
    and minutes.
    """

    __visit_name__ = 'INTERVAL_HOUR_TO_MINUTE'

    def __init__(self, precision=None, **kwargs):
        """Construct an INTERVAL_HOUR_TO_MINUTE object.

        Args:
            precision (int): The permitted range of digits for HOUR, ranging
                from 1 to 4. The default precision is 2.
        """

        super(INTERVAL_HOUR_TO_MINUTE, self).__init__(precision=precision)

    def bind_processor(self, dialect):
        """Handles the processing of datetime.timedelta objects for inserting
        into columns of type INTERVAL HOUR TO MINUTE.

        Timedelta objects are converted to Teradata Interval objects
        appropriate for inserting into INTERVAL HOUR TO MINUTE columns.
        """

        def process(value):
            if isinstance(value, datetime.timedelta):
                hours, seconds = divmod(value.total_seconds(), 3600)
                hours = int(hours)
                minutes = int(seconds / 60)
                value = td_dtypes.Interval(hours=hours, minutes=minutes)
            return value

        return process


class INTERVAL_HOUR_TO_SECOND(_TDInterval):
    """Teradata INTERVAL HOUR TO SECOND data type.

    Identifies a field as an INTERVAL value defining a period of time in hours,
    minutes, and seconds.
    """

    __visit_name__ = 'INTERVAL_HOUR_TO_SECOND'

    def __init__(self, precision=None, frac_precision=None, **kwargs):
        """Construct an INTERVAL_HOUR_TO_SECOND object.

        Args:
            precision (int): The permitted range of digits for HOUR, ranging
                from 1 to 4. The default precision is 2.
            frac_precision (int): The fractional precision for the values of
                SECOND, ranging from 0 to 6. The default fractional precision
                is 6.
        """

        super(INTERVAL_HOUR_TO_SECOND, self).__init__(
            precision=precision, frac_precision=frac_precision)

    def bind_processor(self, dialect):
        """Handles the processing of datetime.timedelta objects for inserting
        into columns of type INTERVAL HOUR TO SECOND.

        Timedelta objects are converted to Teradata Interval objects
        appropriate for inserting into INTERVAL HOUR TO SECOND columns.
        """

        def process(value):
            if isinstance(value, datetime.timedelta):
                hours, seconds = divmod(value.total_seconds(), 3600)
                hours, seconds = int(hours), int(seconds)
                seconds += value.microseconds / 1000000
                value = td_dtypes.Interval(hours=hours, seconds=seconds)
            return value

        return process


class INTERVAL_MINUTE(_TDInterval):
    """Teradata INTERVAL MINUTE data type.

    Identifies a field as an INTERVAL value defining a period of time in
    minutes.
    """

    __visit_name__ = 'INTERVAL_MINUTE'

    def __init__(self, precision=None, **kwargs):
        """Construct an INTERVAL_MINUTE object.

        Args:
            precision (int): The permitted range of digits for MINUTE, ranging
                from 1 to 4. The default precision is 2.
        """

        super(INTERVAL_MINUTE, self).__init__(precision=precision)

    def bind_processor(self, dialect):
        """Handles the processing of datetime.timedelta objects for inserting
        into columns of type INTERVAL MINUTE.

        Timedelta objects are converted to Teradata Interval objects
        appropriate for inserting into INTERVAL MINUTE columns.
        """

        def process(value):
            if isinstance(value, datetime.timedelta):
                minutes = int(value.total_seconds() / 60)
                value = td_dtypes.Interval(minutes=minutes)
            return value

        return process


class INTERVAL_MINUTE_TO_SECOND(_TDInterval):
    """Teradata INTERVAL MINUTE TO SECOND data type.

    Identifies a field as an INTERVAL value defining a period of time in
    minutes and seconds.
    """

    __visit_name__ = 'INTERVAL_MINUTE_TO_SECOND'

    def __init__(self, precision=None, frac_precision=None, **kwargs):
        """Construct an INTERVAL_MINUTE_TO_SECOND object.

        Args:
            precision (int): The permitted range of digits for MINUTE, ranging
                from 1 to 4. The default precision is 2.
            frac_precision (int): The fractional precision for the values of
                SECOND, ranging from 0 to 6. The default fractional precision
                is 6.
        """

        super(INTERVAL_MINUTE_TO_SECOND, self).__init__(
            precision=precision, frac_precision=frac_precision)

    def bind_processor(self, dialect):
        """Handles the processing of datetime.timedelta objects for inserting
        into columns of type INTERVAL MINUTE TO SECOND.

        Timedelta objects are converted to Teradata Interval objects
        appropriate for inserting into INTERVAL MINUTE TO SECOND columns.
        """

        def process(value):
            if isinstance(value, datetime.timedelta):
                minutes, seconds = divmod(value.total_seconds(), 60)
                minutes = int(minutes)
                seconds = int(seconds) + value.microseconds / 1000000
                value = td_dtypes.Interval(minutes=minutes, seconds=seconds)
            return value

        return process


class INTERVAL_SECOND(_TDInterval):
    """Teradata INTERVAL SECOND data type.

    Identifies a field as an INTERVAL value defining a period of time in
    seconds.
    """

    __visit_name__ = 'INTERVAL_SECOND'

    def __init__(self, precision=None, frac_precision=None, **kwargs):
        """Construct an INTERVAL_SECOND object.

        Args:
            precision (int): The permitted range of digits for SECOND, ranging
                from 1 to 4. The default value is 2.
            frac_precision (int): The fractional precision for the values of
                SECOND, ranging from 0 to 6. The default fractional precision
                is 6.
        """

        super(INTERVAL_SECOND, self).__init__(precision=precision,
                                              frac_precision=frac_precision)

    def bind_processor(self, dialect):
        """Handles the processing of datetime.timedelta objects for inserting
        into columns of type INTERVAL SECOND.

        Timedelta objects are converted to Teradata Interval objects
        appropriate for inserting into INTERVAL SECOND columns.
        """

        def process(value):
            if isinstance(value, datetime.timedelta):
                seconds = value.total_seconds()
                value = td_dtypes.Interval(seconds=seconds)
            return value

        return process


class _TDPeriod(_TDType, types.UserDefinedType):
    """Base class for the Teradata PERIOD data types."""

    def __init__(self, format=None, **kwargs):
        """Construct a PERIOD object.

        Args:
            format (str): The format of the DateTime defining the PERIOD type.
        """

        self.format = format


class PERIOD_DATE(_TDPeriod):
    """Teradata PERIOD(DATE) data type.

    Identifies a field as a PERIOD value defining a period of time with a
    beginning and end date.
    """

    __visit_name__ = 'PERIOD_DATE'

    def __init__(self, format=None, **kwargs):
        """Construct a PERIOD_DATE object.

        Args:
            format (str): The format of the date, e.g. 'yyyy-mm-dd'.
        """

        super(PERIOD_DATE, self).__init__(format=format, **kwargs)


class PERIOD_TIME(_TDPeriod):
    """Teradata PERIOD TIME data type.

    Identifies a field as a PERIOD value defining a period of time with a
    beginning and end time.
    """

    __visit_name__ = 'PERIOD_TIME'

    def __init__(self, format=None, frac_precision=None, timezone=False, **kwargs):
        """Construct a PERIOD_TIME object.

        Args:
            format (str): The format of the time, e.g. 'HH:MI:SS.S(6)' and
                'HH:MI:SS.S(6)Z' (with timezone).
            frac_precision (int): The fractional precision for the values of
                SECOND, ranging from 0 to 6. The default fractional precision
                is 6.
            timezone (bool): If set to True creates a PERIOD(TIME WITH TIME
                ZONE) type.
        """

        super(PERIOD_TIME, self).__init__(format=format, **kwargs)
        self.frac_precision = frac_precision
        self.timezone = timezone


class PERIOD_TIMESTAMP(_TDPeriod):
    """Teradata PERIOD TIMESTAMP data type.

    Identifies a field as a PERIOD value defining a period of time with a
    beginning and end timestamp.
    """

    __visit_name__ = 'PERIOD_TIMESTAMP'

    def __init__(self, format=None, frac_precision=None, timezone=False, **kwargs):
        """Construct a PERIOD_TIMESTAMP object.

        Args:
            format (str): The format of the timestamp, e.g. 'YYYY-MM-DDBHH:MI:SS.S(6)'
                and 'YYYY-MM-DDBHH:MI:SS.S(6)Z' (with timezone).
            frac_precision (int): The fractional precision for the values of
                SECOND, ranging from 0 to 6. The default fractional precision
                is 6.
            timezone (bool): If set to True creates a PERIOD(TIMESTAMP WITH TIME
                ZONE) type.
        """

        super(PERIOD_TIMESTAMP, self).__init__(format=format, **kwargs)
        self.frac_precision = frac_precision
        self.timezone = timezone


class CHAR(_TDConcatenable, _TDType, sqltypes.CHAR):
    """Teradata CHAR data type.

    Represents a fixed length character string for Teradata Database internal
    character storage.
    """

    def __init__(self, length=1, charset=None, **kwargs):
        """Construct a CHAR object.

        Args:
            length (int): The number of characters or bytes allotted to the
                column defined with this server character set:

                    'LATIN': The maximum value for `length` is 64000 characters.
                    'UNICODE' and 'GRAPHIC': The maximum value for `length` is
                        32000 characters.
                    'KANJISJIS': The maximum value for `length` is 32000 bytes.

                If a value for `length` is not specified, the default is 1.
            charset (str): The server character set for the character column
                being defined. Supported values for `charset` are as follows:

                    'LATIN': Fixed 8-bit characters from the ASCII ISO 8859
                        Latin1 or ISO 8859 Latin9 repertoires.
                    'UNICODE': Fixed 16-bit characters from the UNICODE 6.0
                        standard.
                    'GRAPHIC': Fixed 16-bit UNICODE characters defined by IBM
                        for DB2.
                    'KANJISJIS': Mixed single byte/multibyte characters
                        intended for Japanese applications that rely on
                        KanjiShiftJIS characteristics.

        Note:
            GRAPHIC(n) is equivalent to CHAR(n) CHARACTER SET GRAPHIC.
        """

        super(CHAR, self).__init__(length=length, **kwargs)
        self.charset = charset


class VARCHAR(_TDConcatenable, _TDType, sqltypes.VARCHAR):
    """Teradata VARCHAR data type.

    Represents a variable length character string of length 0 to `length`
    for Teradata Database internal character storage.
    """

    def __init__(self, length=None, charset=None, **kwargs):
        """Construct a VARCHAR object.

        Args:
            length (int): The maximum number of characters or bytes allotted to
                the column defined with this server character set:

                    'LATIN': The maximum value for `length` is 64000 characters.
                    'UNICODE' and 'GRAPHIC': The maximum value for `length` is
                        32000 characters.
                    'KANJISJIS': The maximum value for `length` is 32000 bytes.

                If a value for `length` is not specified, the default is LONG
                VARCHAR which specifies the longest permissible variable length
                character string for Teradata Database internal character
                storage.
            charset (str): The server character set for the character column
                being defined. Supported values for `charset` are as follows:

                    'LATIN': Fixed 8-bit characters from the ASCII ISO 8859
                        Latin1 or ISO 8859 Latin9 repertoires.
                    'UNICODE': Fixed 16-bit characters from the UNICODE 6.0
                        standard.
                    'GRAPHIC': Fixed 16-bit UNICODE characters defined by IBM
                        for DB2.
                    'KANJISJIS': Mixed single byte/multibyte characters
                        intended for Japanese applications that rely on
                        KanjiShiftJIS characteristics.

        Note:
            VARGRAPHIC(n) is equivalent to VARCHAR(n) CHARACTER SET GRAPHIC.
        """

        super(VARCHAR, self).__init__(length=length, **kwargs)
        self.charset = charset


class CLOB(_TDConcatenable, _TDType, sqltypes.CLOB):
    """Teradata CLOB data type.

    Represents a large character string. A character large object (CLOB) column
    can store character data, such as simple text or HTML.
    """

    def __init__(self, length=None, charset=None, multiplier=None, **kwargs):
        """Construct a CLOB object.

        Args:
            length (int): The number of characters to allocate for the CLOB
                column. The maximum value depends on the server character set:

                    'LATIN': `length` cannot exceed 2097088000.
                    'UNICODE': `length cannot exceed 1048544000.

                If a value for `length` is not specified, the default is the
                maximum value.
            multiplier (str): The multiplier for the number of characters to
                allocate for the CLOB column. Permitted values are as follows:

                    'K': The number of characters to allocate for the CLOB
                        column is nK, where K = 1024 and the maximum value for
                        n is as follows:

                            'LATIN': `length` cannot exceed 2047937.
                            'UNICODE': `length` cannot exceed 1023968.

                    'M': The number of characters to allocate for the CLOB
                        column is nM, where M = 1024K and the maximum value for
                        n is as follows:

                            'LATIN': `length` cannot exceed 1999.
                            'UNICODE': `length` cannot exceed 999.

                    'G': The number of characters to allocate for the CLOB
                        column is nG, where G = 1024M. When G is specified,
                        `length` must be 1 and the server character set must
                        be LATIN.
            charset (str): The server character set for the CLOB column being
                defined:

                    'LATIN': Fixed 8-bit characters from the ASCII ISO 8859
                        Latin1 or ISO 8859 Latin9 repertoires.
                    'UNICODE': Fixed 16-bit characters from the UNICODE 6.0
                        standard.
        """

        super(CLOB, self).__init__(length=length, **kwargs)
        self.charset = charset
        self.multiplier = multiplier


class _TDBinary(_TDConcatenable, _TDType, sqltypes._Binary):
    """Base class for the Teradata binary data types.

    Represents a Teradata binary data type. This includes the BYTE, VARBYTE, and
    BINARY LARGE OBJECT (CLOB) data types. Throws a warning when data may get
    truncated upon insertion into the column.
    """

    class TruncationWarning(UserWarning):
        pass

    def _length(self):
        """Compute the length allocated to this binary column."""

        multiplier_map = {
            'K': 1024,
            'M': 1048576,
            'G': 1073741824,
        }
        if hasattr(self, 'multiplier') and self.multiplier in multiplier_map:
            return self.length * multiplier_map[self.multiplier]

        return self.length

    def bind_processor(self, dialect):
        """Throws a warning when data may get truncated upon insertion into
        this column.

        DBAPI level validation logic which throws a `TruncationWarning` when
        inserting objects larger than the specified number of bytes allocated
        to this column.
        """

        if dialect.dbapi is None:
            return None

        DBAPIBinary = dialect.dbapi.Binary

        def process(value):
            bin_length = self._length()

            if value is not None and bin_length is not None:
                value = DBAPIBinary(value)
                if len(value) > bin_length:
                    warnings.warn(
                        'Attempting to insert an item that is larger than the '
                        'space allocated for this column. Data may get '
                        'truncated.',
                        self.TruncationWarning)
                return value

            return None

        return process


class BYTE(_TDBinary, sqltypes.BINARY):
    """Teradata BYTE data type.

    Represents a fixed-length binary string.
    """

    __visit_name__ = 'BYTE'

    def __init__(self, length=None, **kwargs):
        """Construct a BYTE object.

        Args:
            length (int): The number of bytes in the string. The maximum value
                for `length` is 64000.
        """

        super(BYTE, self).__init__(length=length, **kwargs)

    def literal_processor(self, dialect):
        def process(value):
            try:
                # Python 3.5+
                return "'{}'XB".format(value.hex())

            except AttributeError:
                # Try it with codecs
                import codecs
                return "'{}'XB".format(
                    codecs.encode(value, 'hex').decode('utf-8'))

        return process


class VARBYTE(_TDBinary, sqltypes.VARBINARY):
    """Teradata VARBYTE data type.

    Represents a variable-length binary string.
    """

    __visit_name__ = 'VARBYTE'

    def __init__(self, length=None, **kwargs):
        """Construct a VARBYTE object.

        Args:
            length (int): The number of bytes in the string. The maximum value
                for `length` is 64000.
        """

        super(VARBYTE, self).__init__(length=length, **kwargs)


class BLOB(_TDBinary, sqltypes.BLOB):
    """Teradata BLOB data type.

    Represents a large binary string of raw bytes. A binary large object (BLOB)
    column can store binary objects, such as graphics, video clips, files, and
    documents.
    """

    def __init__(self, length=None, multiplier=None, **kwargs):
        """Construct a BLOB object.

        Args:
            length (int): The number of bytes to allocate for the BLOB column.
                The maximum number of bytes is 2097088000, which is the default
                if `length` is not specified.
            multiplier (str): The multiplier for the number of bytes to
                allocate for the BLOB column. Permitted values are as follows:

                'K': `length` is specified in kilobytes (KB). When K is
                    specified, `length` cannot exceed 2047937.
                'M': `length` is specified in megabytes (Mb). When M is
                    specified, `length` cannot exceed 1999.
                'G': `length` is specified in gigabytes (GB). When G is
                    specified, `length` must be 1.

        Note:
            If you specify a multiplier without specifying the length, the
            multiplier argument will simply get ignored. On the other hand,
            specifying a length without a multiplier will implicitly indicate
            that the length value should be interpreted as bytes (B).
        """

        super(BLOB, self).__init__(length=length, **kwargs)
        self.multiplier = multiplier


class TeradataExpressionAdapter:
    """Expression adapter for Teradata data types.

    For inferring the resulting type of a BinaryExpression whose operation
    involves operands that are Teradata types.
    """

    def process(self, type_, op=None, other=None, **kw):
        """Adapts the expression.

        Infer the type of the resultant BinaryExpression defined by the passed
        in operator and operands. This resulting type should be consistent with
        the Teradata database when the operation is well-defined.

        Args:
            type_: The type instance of the left operand.
            op: The operator of the BinaryExpression.
            other: The type instance of the right operand.

        Returns:
            The type to adapt the BinaryExpression to.
        """

        if isinstance(type_, _TDInterval) or isinstance(other, _TDInterval):
            adapt_strategy = _IntervalRuleStrategy()
        else:
            adapt_strategy = _LookupStrategy()

        return adapt_strategy.adapt(type_, op, other, **kw)


class _AdaptStrategy:
    """Interface for expression adaptation strategies."""

    def adapt(self, type_, op, other, **kw):
        """Adapts the expression according to some strategy.

        Given the type of the left and right operand, and the operator, produce
        a type class for the resulting BinaryExpression.
        """

        raise NotImplementedError()


class _IntervalRuleStrategy(_AdaptStrategy):
    """Expression adaptation strategy which follows a set of rules for inferring
    the types of expressions involving Teradata Interval types.
    """

    ordering = ('YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND')

    def adapt(self, type_, op, other, **kw):
        """Adapts the expression by a set of predefined rules over the Teradata
        Interval types.
        """

        # If the (Interval) types are equal, simply return the class of
        # those types
        if type_.__class__ == other.__class__:
            return type_.__class__

        # If the (Interval) types are not equal, return the valid Interval type
        # with the greatest range.
        #
        # E.g. INTERVAL YEAR TO MONTH and INTERVAL DAY TO HOUR -->
        #      INTERVAL YEAR TO HOUR.
        #
        # Otherwise if the resulting Interval type is invalid, return NullType.
        #
        # E.g. INTERVAL YEAR TO MONTH and INTERVAL MINUTE TO SECOND -->
        #      INTERVAL YEAR TO SECOND (invalid) -->
        #      NullType
        if isinstance(type_, _TDInterval) and isinstance(other, _TDInterval):
            tokens = self._tokenize_name(type_.__class__.__name__) + \
                     self._tokenize_name(other.__class__.__name__)
            tokens.sort(key=lambda tok: self.ordering.index(tok))

            return getattr(sys.modules[__name__],
                           self._combine_tokens(tokens[0], tokens[-1]),
                           sqltypes.NullType)()

        # Else the binary expression has an Interval and non-Interval operand.
        # If the non-Interval operand is a Date, Time, or Datetime, return that
        # type, otherwise return the Interval type.
        interval, non_interval = ((type_, other)
                                  if isinstance(type_, _TDInterval)
                                  else (other, type_))

        return (non_interval.__class__
                if isinstance(non_interval, (sqltypes.Date,
                                             sqltypes.Time,
                                             sqltypes.DateTime))
                else interval.__class__)

    def _tokenize_name(self, interval_name):
        """Tokenize the name of Interval types.

        Returns:
            A list of (str) tokens of the corresponding Interval type name.

        Example:
            'INTERVAL_DAY_TO_HOUR' --> ['DAY', 'HOUR'].
        """

        return list(filter(lambda tok: tok not in ('INTERVAL', 'TO'),
                           interval_name.split('_')))

    def _combine_tokens(self, tok_l, tok_r):
        """Combine the tokens of an Interval type to form its name.

        Returns:
            A string for the name of the Interval type corresponding to the
            tokens passed in.

        Example:
            tok_l='DAY' and tok_r='HOUR' --> 'INTERVAL_DAY_TO_HOUR'
        """

        return 'INTERVAL_{}_TO_{}'.format(tok_l, tok_r)


class _LookupStrategy(_AdaptStrategy):
    """Expression adaptation strategy which employs a general lookup table."""

    def adapt(self, type_, op, other, **kw):
        """Adapts the expression by looking up a hardcoded table.

        The lookup table is defined as the set of `visit_` methods below. Each
        method returns a nested dictionary which is keyed by the operator and
        the other operand's type.
        """

        return getattr(self, self._process_visit_name(type_.__visit_name__),
                       lambda *args, **kw: {})(type_, other, **kw) \
            .get(op, util.immutabledict()) \
            .get(other.__class__, type_.__class__)

    def _process_visit_name(self, visit_name):
        """Generate the corresponding visit function name from a type's
        __visit_name__ field.
        """

        prefix = 'visit_'
        return prefix + visit_name

    def _flatten_tuple_keyed_dict(self, tuple_dict):
        """Recursively flatten a dictionary with (many-to-one) tuple keys to a
        standard one.
        """

        flat_dict = {}
        for ks, v in tuple_dict.items():
            v = self._flatten_tuple_keyed_dict(v) if isinstance(v, dict) else v
            if isinstance(ks, tuple):
                for k in ks:
                    flat_dict[k] = v
            else:
                flat_dict[ks] = v

        return flat_dict

    def visit_INTEGER(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.add: {
                (CHAR, VARCHAR, BLOB): FLOAT,
                BIGINT: BIGINT,
                DECIMAL: DECIMAL,
                FLOAT: FLOAT,
                NUMBER: NUMBER,
                DATE: DATE,
            },
            (operators.sub, operators.mul, operators.truediv,
             operators.mod): {
                 (CHAR, VARCHAR, BLOB): FLOAT,
                 BIGINT: BIGINT,
                 DECIMAL: DECIMAL,
                 FLOAT: FLOAT,
                 NUMBER: NUMBER,
             }
        })

    def visit_SMALLINT(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.add: {
                (BYTEINT, SMALLINT, INTEGER, DATE): INTEGER,
                (CHAR, VARCHAR, BLOB): FLOAT,
                BIGINT: BIGINT,
                DECIMAL: DECIMAL,
                FLOAT: FLOAT,
                NUMBER: NUMBER,
                DATE: DATE,
            },
            (operators.sub, operators.mul, operators.truediv,
             operators.mod): {
                 (BYTEINT, SMALLINT, INTEGER, DATE): INTEGER,
                 (CHAR, VARCHAR, BLOB): FLOAT,
                 BIGINT: BIGINT,
                 DECIMAL: DECIMAL,
                 FLOAT: FLOAT,
                 NUMBER: NUMBER,
             }
        })

    def visit_BIGINT(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.add: {
                (CHAR, VARCHAR, BLOB): FLOAT,
                DECIMAL: DECIMAL,
                FLOAT: FLOAT,
                NUMBER: NUMBER,
                DATE: DATE,
            },
            (operators.sub, operators.mul, operators.truediv,
             operators.mod): {
                 (CHAR, VARCHAR, BLOB): FLOAT,
                 DECIMAL: DECIMAL,
                 FLOAT: FLOAT,
                 NUMBER: NUMBER,
             }
        })

    def visit_BYTEINT(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            (operators.add, operators.sub): {
                (BYTEINT, SMALLINT, INTEGER): INTEGER,
                (CHAR, VARCHAR, BLOB): FLOAT,
                BIGINT: BIGINT,
                DECIMAL: DECIMAL,
                FLOAT: FLOAT,
                NUMBER: NUMBER,
                DATE: DATE,
            },
            (operators.mul, operators.truediv, operators.mod): {
                (BYTEINT, SMALLINT, INTEGER, DATE): INTEGER,
                (CHAR, VARCHAR, BLOB): FLOAT,
                BIGINT: BIGINT,
                DECIMAL: DECIMAL,
                FLOAT: FLOAT,
                NUMBER: NUMBER,
            }
        })

    def visit_DECIMAL(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.add: {
                (CHAR, VARCHAR, BLOB): FLOAT,
                FLOAT: FLOAT,
                NUMBER: NUMBER,
                DATE: DATE,
            },
            (operators.sub, operators.mul, operators.truediv,
             operators.mod): {
                 (CHAR, VARCHAR, BLOB): FLOAT,
                 FLOAT: FLOAT,
                 NUMBER: NUMBER,
             }
        })

    def visit_NUMBER(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.add: {
                (CHAR, VARCHAR, BLOB): FLOAT,
                FLOAT: FLOAT,
                DATE: DATE,
            },
            (operators.sub, operators.mul, operators.truediv,
             operators.mod): {
                 (CHAR, VARCHAR, BLOB): FLOAT,
                 FLOAT: FLOAT,
             }
        })

    def visit_DATE(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.add: {
                (CHAR, VARCHAR, BLOB): FLOAT,
                FLOAT: FLOAT,
                DATE: INTEGER,
            },
            operators.sub: {
                (CHAR, VARCHAR, BLOB): FLOAT,
                FLOAT: FLOAT,
                DATE: INTEGER,
            },
            (operators.mul, operators.truediv, operators.mod): {
                (BYTEINT, SMALLINT, INTEGER, DATE): INTEGER,
                (CHAR, VARCHAR, BLOB): FLOAT,
                (FLOAT, TIME): FLOAT,
                BIGINT: BIGINT,
                DECIMAL: DECIMAL,
                NUMBER: NUMBER,
            }
        })

    def visit_TIME(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            (operators.add, operators.mul, operators.truediv,
             operators.mod): {
                 DATE: FLOAT,
             }
        })

    def visit_CHAR(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.concat_op: {
                # Attribute dependent
                CHAR: (VARCHAR
                       if (hasattr(other, 'charset')
                           and ((type_.charset == 'unicode') !=
                                (other.charset == 'unicode')))
                       else CHAR),
                VARCHAR: VARCHAR,
                CLOB: CLOB,
            },
            (operators.add, operators.sub, operators.mul,
             operators.truediv, operators.mod): {
                 (BYTEINT, SMALLINT, INTEGER, BIGINT, DECIMAL, FLOAT, NUMBER,
                  DATE, CHAR, VARCHAR): FLOAT,
             }
        })

    def visit_VARCHAR(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.concat_op: {
                CLOB: CLOB,
            },
            (operators.add, operators.sub, operators.mul,
             operators.truediv, operators.mod): {
                 (BYTEINT, SMALLINT, INTEGER, BIGINT, DECIMAL, FLOAT, NUMBER,
                  DATE, CHAR, VARCHAR): FLOAT,
             }
        })

    def visit_BYTE(self, type_, other, **kw):
        return {
            operators.concat_op: {
                VARBYTE: VARBYTE,
                BLOB: BLOB
            }
        }

    def visit_VARBYTE(self, type_, other, **kw):
        return {
            operators.concat_op: {
                BLOB: BLOB
            }
        }
