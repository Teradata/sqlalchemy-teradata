# sqlalchemy_teradata/types.py
# Copyright (C) 2015-2016 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import util
from sqlalchemy import types
from sqlalchemy.sql import sqltypes, operators

import datetime, decimal
import warnings
import teradata.datatypes as td_dtypes


class _TDComparable:
    """Teradata Comparable Data Type."""

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
    """Teradata Concatenable Data Type.

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
                operators.concat_op if op is operators.add and
                    isinstance(other_comparator.type, self.type._type_affinity)
                else op, other_comparator)

    comparator_factory = Comparator


class _TDLiteralCoercer:
    """Mixin for literal type processing against Teradata data types."""

    def coerce_compared_value(self, op, value):
        type_ = type(value)

        if type_ == int:
            return INTEGER()
        elif type_ == float:
            return FLOAT()
        elif type_ == bytes:
            return BYTE()
        elif type_ == str:
            return VARCHAR()
        elif type_ == decimal.Decimal:
            return DECIMAL()
        elif type_ == datetime.date:
            return DATE()
        elif type_ == datetime.datetime:
            return TIMESTAMP()
        elif type_ == datetime.time:
            return TIME()
        elif type_ == td_dtypes.Interval:
            return globals().get(
                'INTERVAL_' + value.type.replace(' ', '_'),
                sqltypes.NullType)()
        # TODO PERIOD

        return sqltypes.NullType()


class _TDType(_TDLiteralCoercer, _TDComparable):

    """ Teradata Data Type

    Identifies a Teradata data type. Currently used to override __str__
    behavior such that the type will get printed without being compiled by the
    GenericTypeCompiler (which would otherwise result in an exception).
    """

    def _parse_name(self, name):
        return name.replace('_', ' ')

    def __str__(self):
        return self._parse_name(self.__class__.__name__)


class INTEGER(_TDType, sqltypes.INTEGER):

    """ Teradata INTEGER type

    Represents a signed, binary integer value from -2,147,483,648 to
    2,147,483,647.

    """

    def __init__(self, **kwargs):

        """ Construct a INTEGER Object """
        super(INTEGER, self).__init__(**kwargs)


class SMALLINT(_TDType, sqltypes.SMALLINT):

    """ Teradata SMALLINT type

    Represents a signed binary integer value in the range -32768 to 32767.

    """

    def __init__(self, **kwargs):

        """ Construct a SMALLINT Object """
        super(SMALLINT, self).__init__(**kwargs)


class BIGINT(_TDType, sqltypes.BIGINT):

    """ Teradata BIGINT type

    Represents a signed, binary integer value from -9,223,372,036,854,775,808
    to 9,223,372,036,854,775,807.

    """

    def __init__(self, **kwargs):

        """ Construct a BIGINT Object """
        super(BIGINT, self).__init__(**kwargs)


class DECIMAL(_TDType, sqltypes.DECIMAL):

    """ Teradata DECIMAL type

    Represents a decimal number of n digits, with m of those n digits to the
    right of the decimal point.

    """

    def __init__(self, precision = 5, scale = 0, **kwargs):

        """ Construct a DECIMAL Object """
        super(DECIMAL, self).__init__(**kwargs)

    def literal_processor(self, dialect):

        def process(value):
            return str(value) + ('' if value.as_tuple()[2] < 0 else '.')
        return process


class BYTEINT(_TDType, sqltypes.Integer):

    """ Teradata BYTEINT type

    This type represents a one byte signed integer.

    """

    __visit_name__ = 'BYTEINT'

    def __init__(self, **kwargs):

        """ Construct a BYTEINT Object """
        super(BYTEINT, self).__init__(**kwargs)


class _TDBinary(_TDConcatenable, _TDType, sqltypes._Binary):

    """ Teradata Binary Types

    This type represents a Teradata binary string. Warns users when
    data may get truncated upon insertion.

    """

    class TruncationWarning(UserWarning):
        pass

    def _length(self):
        """Compute the length allocated to this binary column."""

        multiplier_map = {
            'K': 1024,
            'M': 1048576,
            'G': 1073741824
        }
        if hasattr(self, 'multiplier') and self.multiplier in multiplier_map:
            return self.length * multiplier_map[self.multiplier]

        return self.length

    def bind_processor(self, dialect):
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
                        'space allocated for this column. Data may get truncated.',
                        self.TruncationWarning)
                return value
            else:
                return None
        return process


class BYTE(_TDBinary, sqltypes.BINARY):

    """ Teradata BYTE type

    This type represents a fixed-length binary string and is equivalent to
    the BINARY SQL standard type.

    """

    __visit_name__ = 'BYTE'

    def __init__(self, length=None, **kwargs):

        """ Construct a BYTE object

        :param length: Optional 1 to n. Specifies the number of bytes in the
        fixed-length binary string. The maximum value for n is 64000.

        """
        super(BYTE, self).__init__(length=length, **kwargs)

    def literal_processor(self, dialect):

        def process(value):
            return "'%s'XB" % value.hex()
        return process


class VARBYTE(_TDBinary, sqltypes.VARBINARY):

    """ Teradata VARBYTE type

    This type represents a variable-length binary string and is equivalent to
    the VARBINARY SQL standard type.

    """

    __visit_name__ = 'VARBYTE'

    def __init__(self, length=None, **kwargs):

        """ Construct a VARBYTE object

        :param length: Optional 1 to n. Specifies the number of bytes in the
        fixed-length binary string. The maximum value for n is 64000.

        """
        super(VARBYTE, self).__init__(length=length, **kwargs)


class BLOB(_TDBinary, sqltypes.BLOB):

    """ Teradata BLOB type

    This type represents a large binary string of raw bytes. A binary large
    object (BLOB) column can store binary objects, such as graphics, video
    clips, files, and documents.

    """

    def __init__(self, length=None, multiplier=None, **kwargs):

        """ Construct a BLOB object

        :param length: Optional 1 to n. Specifies the number of bytes allocated
        for the BLOB column. The maximum number of bytes is 2097088000, which
        is the default if n is not specified.

        :param multiplier: Optional value in ('K', 'M', 'G'). Indicates that the
        length parameter n is specified in kilobytes (KB), megabytes (Mb),
        or gigabytes (GB) respectively. Note the following constraints on n
        hold for each of the allowable multiplier:

            'K' is specified, n cannot exceed 2047937.
            'M' is specified, n cannot exceed 1999.
            'G' is specified, n must be 1.

        If multiplier is None, the length is interepreted as bytes (B).

        Note: If you specify a multiplier without specifying the length, the
              multiplier argument will simply get ignored. On the other hand,
              specifying a length without a multiplier will implicitly indicate
              that the length value should be interpreted as bytes (B).

        """
        super(BLOB, self).__init__(length=length, **kwargs)
        self.multiplier = multiplier


class FLOAT(_TDType, sqltypes.FLOAT):

    """ Teradata FLOAT type

    This type represent values in sign/magnitude form ranging from
    2.226 x 10^-308 to 1.797 x 10^308.

    """

    def __init__(self, **kwargs):

        """ Construct a FLOAT object """
        super(FLOAT, self).__init__(**kwargs)

    def literal_processor(self, dialect):

        def process(value):
            return "%.14e" % value
        return process


class NUMBER(_TDType, sqltypes.NUMERIC):

    """ Teradata NUMBER type

    This type represents a numeric value with optional precision and scale
    limitations.

    """

    __visit_name__ = 'NUMBER'

    def __init__(self, precision=None, scale=None, **kwargs):

        """ Construct a NUMBER object

        :param precision: max number of digits that can be stored. Valid values
        range from 1 to 38.

        :param scale: number of fractional digits of :param precision: to the
        right of the decimal point. Valid values range from 0 to
        :param precision:.

        Note: Both parameters are optional. When both are left unspecified,
              defaults to NUMBER with the system limits for precision and scale.

        """
        super(NUMBER, self).__init__(precision=precision, scale=scale, **kwargs)


class DATE(_TDType, sqltypes.DATE):

    """ Teradata DATE type

    Identifies a field as a DATE value and simplifies handling and formatting
    of date variables.

    """

    def __init__(self, **kwargs):

        """ Construct a DATE Object """
        super(DATE, self).__init__(**kwargs)

    def literal_processor(self, dialect):

        def process(value):
            return "DATE '%s'" % value
        return process


class TIME(_TDType, sqltypes.TIME):

    """ Teradata TIME type

    This type identifies a field as a TIME value.

    """

    def __init__(self, precision=6, timezone=False, **kwargs):

        """ Construct a TIME stored as UTC in Teradata

        :param precision: optional fractional seconds precision. A single digit
        representing the number of significant digits in the fractional
        portion of the SECOND field. Valid values range from 0 to 6 inclusive.
        The default precision is 6.

        :param timezone: If set to True creates a Time WITH TIME ZONE type

        """
        super(TIME, self).__init__(timezone=timezone, **kwargs)
        self.precision = precision

    def literal_processor(self, dialect):

        def process(value):
            return "TIME '%s'" % value
        return process


class TIMESTAMP(_TDType, sqltypes.TIMESTAMP):

    """ Teradata TIMESTAMP type

    This type identifies a field as a TIMESTAMP value.

    """

    def __init__(self, precision=6, timezone=False, **kwargs):
        """ Construct a TIMESTAMP stored as UTC in Teradata

        :param precision: optional fractional seconds precision. A single digit
        representing the number of significant digits in the fractional
        portion of the SECOND field. Valid values range from 0 to 6 inclusive.
        The default precision is 6.

        :param timezone: If set to True creates a TIMESTAMP WITH TIME ZONE type

        """
        super(TIMESTAMP, self).__init__(timezone=timezone, **kwargs)
        self.precision = precision

    def literal_processor(self, dialect):

        def process(value):
            return "TIMESTAMP '%s'" % value
        return process

    def get_dbapi_type(self, dbapi):
      return dbapi.DATETIME


class _TDInterval(_TDType, types.UserDefinedType):

    """ Base class for the Teradata INTERVAL sqltypes """

    def __init__(self, precision=None, frac_precision=None, **kwargs):
        self.precision      = precision
        self.frac_precision = frac_precision

    def bind_processor(self, dialect):

        """
        Processes the Interval value from SQLAlchemy to DB
        """
        def process(value):
            return value
        return process

    def result_processor(self, dialect, coltype):

        """
        Processes the Interval value from DB to SQLAlchemy
        """
        def process(value):
            return value
        return process

    def literal_processor(self, dialect):

        def process(value):
            return "INTERVAL '%s' %s" % (value, value.type)
        return process

class INTERVAL_YEAR(_TDInterval):

    """ Teradata INTERVAL YEAR type

    This type identifies a field defining a period of time in years.

    """
    __visit_name__ = 'INTERVAL_YEAR'

    def __init__(self, precision=None, **kwargs):

       """ Construct an INTERVAL_YEAR object

       :param precision: permitted range of digits for year ranging from 1 to 4

       """
       super(INTERVAL_YEAR, self).__init__(precision=precision)

class INTERVAL_YEAR_TO_MONTH(_TDInterval):

    """ Teradata INTERVAL YEAR TO MONTH type

    This type identifies a field defining a period of time in years and months.

    """

    __visit_name__ = 'INTERVAL_YEAR_TO_MONTH'

    def __init__(self, precision=None, **kwargs):

        """ Construct an INTERVAL_YEAR_TO_MONTH object

        :param precision: permitted range of digits for year ranging from 1 to 4

        """
        super(INTERVAL_YEAR_TO_MONTH, self).__init__(precision=precision)

class INTERVAL_MONTH(_TDInterval):

    """ Teradata INTERVAL MONTH type

    This type identifies a field defining a period of time in months.

    """

    __visit_name__ = 'INTERVAL_MONTH'

    def __init__(self, precision=None, **kwargs):

        """ Construct an INTERVAL_MONTH object

        :param precision: permitted range of digits for month ranging from 1 to 4

        """
        super(INTERVAL_MONTH, self).__init__(precision=precision)

class INTERVAL_DAY(_TDInterval):

    """ Teradata INTERVAL DAY type

    This type identifies a field defining a period of time in days.

    """

    __visit_name__ = 'INTERVAL_DAY'

    def __init__(self, precision=None, **kwargs):

        """ Construct an INTERVAL_DAY object

        :param precision: permitted range of digits for day ranging from 1 to 4

        """
        super(INTERVAL_DAY, self).__init__(precision=precision)

    def bind_processor(self, dialect):

        """
        Handles the conversion from a datetime.timedelta object to an Interval
        object appropriate for inserting into a column with type INTERVAL DAY

        """
        def process(value):
            if isinstance(value, datetime.timedelta):
                value = td_dtypes.Interval(days=value.days)
            return value
        return process

class INTERVAL_DAY_TO_HOUR(_TDInterval):

    """ Teradata INTERVAL DAY TO HOUR type

    This type identifies a field defining a period of time in days and hours.

    """

    __visit_name__ = 'INTERVAL_DAY_TO_HOUR'

    def __init__(self, precision=None, **kwargs):

        """ Construct an INTERVAL_DAY_TO_HOUR object

        :param precision: permitted range of digits for day ranging from 1 to 4

        """
        super(INTERVAL_DAY_TO_HOUR, self).__init__(precision=precision)

    def bind_processor(self, dialect):

        """
        Handles the conversion from a datetime.timedelta object to an Interval
        object appropriate for inserting into a column with type INTERVAL DAY
        TO HOUR

        """
        def process(value):
            if isinstance(value, datetime.timedelta):
                hours = int(value.seconds / 3600)
                value = td_dtypes.Interval(days=value.days, hours=hours)
            return value
        return process

class INTERVAL_DAY_TO_MINUTE(_TDInterval):

    """ Teradata INTERVAL DAY TO MINUTE type

    This type identifies a field defining a period of time in days, hours,
    and minutes.

    """

    __visit_name__ = 'INTERVAL_DAY_TO_MINUTE'

    def __init__(self, precision=None, **kwargs):

        """ Construct an INTERVAL_DAY_TO_MINUTE object

        :param precision: permitted range of digits for day ranging from 1 to 4

        """
        super(INTERVAL_DAY_TO_MINUTE, self).__init__(precision=precision)

    def bind_processor(self, dialect):

        """
        Handles the conversion from a datetime.timedelta object to an Interval
        object appropriate for inserting into a column with type INTERVAL DAY
        TO MINUTE

        """
        def process(value):
            if isinstance(value, datetime.timedelta):
                minutes = int(value.seconds / 60)
                value   = td_dtypes.Interval(days=value.days, minutes=minutes)
            return value
        return process

class INTERVAL_DAY_TO_SECOND(_TDInterval):

    """ Teradata INTERVAL DAY TO SECOND type

    This type identifies a field during a period of time in days, hours, minutes,
    and seconds.

    """

    __visit_name__ = 'INTERVAL_DAY_TO_SECOND'

    def __init__(self, precision=None, frac_precision=None, **kwargs):

        """ Construct an INTERVAL_DAY_TO_SECOND object

        :param precision: permitted range of digits for day ranging from 1 to 4

        :param frac_precision: fracional_seconds_precision ranging from 0 to 6

        """
        super(INTERVAL_DAY_TO_SECOND, self).__init__(precision=precision,
                                                     frac_precision=frac_precision)

    def bind_processor(self, dialect):

        """
        Handles the conversion from a datetime.timedelta object to an Interval
        object appropriate for inserting into a column with type INTERVAL DAY
        TO SECOND

        """
        def process(value):
            if isinstance(value, datetime.timedelta):
                seconds = value.seconds + value.microseconds / 1000000
                value   = td_dtypes.Interval(days=value.days, seconds=seconds)
            return value
        return process

class INTERVAL_HOUR(_TDInterval):

    """ Teradata INTERVAL HOUR type

    This type identifies a field defining a period of time in hours.

    """

    __visit_name__ = 'INTERVAL_HOUR'

    def __init__(self, precision=None, **kwargs):

        """ Construct an INTERVAL_HOUR object

        :param precision: permitted range of digits for hour ranging from 1 to 4

        """
        super(INTERVAL_HOUR, self).__init__(precision=precision)

    def bind_processor(self, dialect):

        """
        Handles the conversion from a datetime.timedelta object to an Interval
        object appropriate for inserting into a column with type INTERVAL HOUR

        """
        def process(value):
            if isinstance(value, datetime.timedelta):
                hours = int(value.total_seconds() / 3600)
                value = td_dtypes.Interval(hours=hours)
            return value
        return process

class INTERVAL_HOUR_TO_MINUTE(_TDInterval):

    """ Teradata INTERVAL HOUR TO MINUTE type

    This type identifies a field defining a period of time in hours and minutes.

    """

    __visit_name__ = 'INTERVAL_HOUR_TO_MINUTE'

    def __init__(self, precision=None, **kwargs):

        """ Construct an INTERVAL_HOUR_TO_MINUTE object

        :param precision: permitted range of digits for hour ranging from 1 to 4

        """
        super(INTERVAL_HOUR_TO_MINUTE, self).__init__(precision=precision)

    def bind_processor(self, dialect):

        """
        Handles the conversion from a datetime.timedelta object to an Interval
        object appropriate for inserting into a column with type INTERVAL HOUR
        TO MINUTE

        """
        def process(value):
            if isinstance(value, datetime.timedelta):
                hours, seconds = divmod(value.total_seconds(), 3600)
                hours   = int(hours)
                minutes = int(seconds / 60)
                value   = td_dtypes.Interval(hours=hours, minutes=minutes)
            return value
        return process

class INTERVAL_HOUR_TO_SECOND(_TDInterval):

    """ Teradata INTERVAL HOUR TO SECOND type

    This type identifies a field defining a period of time in hours, minutes,
    and seconds.

    """

    __visit_name__ = 'INTERVAL_HOUR_TO_SECOND'

    def __init__(self, precision=None, frac_precision=None, **kwargs):

        """ Construct an INTERVAL_HOUR_TO_SECOND object

        :param precision: permitted range of digits for hour ranging from 1 to 4

        :param frac_precision: fracional_seconds_precision ranging from 0 to 6

        """
        super(INTERVAL_HOUR_TO_SECOND, self).__init__(precision=precision,
                                                      frac_precision=frac_precision)

    def bind_processor(self, dialect):

        """
        Handles the conversion from a datetime.timedelta object to an Interval
        object appropriate for inserting into a column with type INTERVAL HOUR
        TO SECOND

        """
        def process(value):
            if isinstance(value, datetime.timedelta):
                hours, seconds = divmod(value.total_seconds(), 3600)
                hours   = int(hours)
                seconds = int(seconds) + value.microseconds / 1000000
                value   = td_dtypes.Interval(hours=hours, seconds=seconds)
            return value
        return process

class INTERVAL_MINUTE(_TDInterval):

    """ Teradata INTERVAL MINUTE type

    This type identifies a field defining a period of time in minutes.

    """

    __visit_name__ = 'INTERVAL_MINUTE'

    def __init__(self, precision=None, **kwargs):

        """ Construct an INTERVAL_MINUTE object

        :param precision: permitted range of digits for minute ranging from 1 to 4

        """
        super(INTERVAL_MINUTE, self).__init__(precision=precision)

    def bind_processor(self, dialect):

        """
        Handles the conversion from a datetime.timedelta object to an Interval
        object appropriate for inserting into a column with type INTERVAL MINUTE

        """
        def process(value):
            if isinstance(value, datetime.timedelta):
                minutes = int(value.total_seconds() / 60)
                value = td_dtypes.Interval(minutes=minutes)
            return value
        return process

class INTERVAL_MINUTE_TO_SECOND(_TDInterval):

    """ Teradata INTERVAL MINUTE TO SECOND type

    This type identifies a field defining a period of time in minutes and seconds.

    """

    __visit_name__ = 'INTERVAL_MINUTE_TO_SECOND'

    def __init__(self, precision=None, frac_precision=None, **kwargs):

        """ Construct an INTERVAL_MINUTE_TO_SECOND object

        :param precision: permitted range of digits for minute ranging from 1 to 4

        :param frac_precision: fracional_seconds_precision ranging from 0 to 6

        """
        super(INTERVAL_MINUTE_TO_SECOND, self).__init__(precision=precision,
                                                        frac_precision=frac_precision)

    def bind_processor(self, dialect):

        """
        Handles the conversion from a datetime.timedelta object to an Interval
        object appropriate for inserting into a column with type INTERVAL MINUTE
        TO SECOND

        """
        def process(value):
            if isinstance(value, datetime.timedelta):
                minutes, seconds = divmod(value.total_seconds(), 60)
                minutes = int(minutes)
                seconds = int(seconds) + value.microseconds / 1000000
                value   = td_dtypes.Interval(minutes=minutes, seconds=seconds)
            return value
        return process

class INTERVAL_SECOND(_TDInterval):

    """ Teradata INTERVAL SECOND type

    This type identifies a field defining a period of time in seconds.

    """

    __visit_name__ = 'INTERVAL_SECOND'

    def __init__(self, precision=None, frac_precision=None, **kwargs):

        """ Construct an INTERVAL_SECOND object

        :param precision: permitted range of digits for second ranging from 1 to 4

        :param frac_precision: fractional_seconds_precision ranging from 0 to 6

        """
        super(INTERVAL_SECOND, self).__init__(precision=precision,
                                              frac_precision=frac_precision)

    def bind_processor(self, dialect):

        """
        Handles the conversion from a datetime.timedelta object to an Interval
        object appropriate for inserting into a column with type INTERVAL SECOND

        """
        def process(value):
            if isinstance(value, datetime.timedelta):
                seconds = value.total_seconds()
                value = td_dtypes.Interval(seconds=seconds)
            return value
        return process


class _TDPeriod(_TDType, types.UserDefinedType):

    """ Base class for the Teradata Period sqltypes """

    def __init__(self, format=None, **kwargs):
        self.format = format

    def bind_processor(self, dialect):

        """
        Processes the Period value from SQLAlchemy to DB
        """
        def process(value):
            return value
        return process

    def result_processor(self, dialect, coltype):

        """
        Processes the Period value from DB to SQLAlchemy
        """
        def process(value):
            return value
        return process

class PERIOD_DATE(_TDPeriod):

    """ Teradata PERIOD DATE type

    This type identifies a field defining a duration with a beginning and end date.

    """

    __visit_name__ = 'PERIOD_DATE'

    def __init__(self, format=None, **kwargs):

        """ Construct a PERIOD_DATE object

        :param format: format of the date, e.g. 'yyyy-mm-dd'

        """
        super(PERIOD_DATE, self).__init__(format=format, **kwargs)

class PERIOD_TIME(_TDPeriod):

    """ Teradata PERIOD TIME type

    This type identifies a field defining a duration with a beginning and end time.

    """

    __visit_name__ = 'PERIOD_TIME'

    def __init__(self, format=None, frac_precision=None, timezone=False, **kwargs):

        """ Construct a PERIOD_TIME object

        :param format: format of the time, e.g. 'HH:MI:SS.S(6)' and
        'HH:MI:SS.S(6)Z' (with timezone)

        :param frac_precision: fractional_seconds_precision ranging from 0 to 6

        :param timezone: true if WITH TIME ZONE, false otherwise

        """
        super(PERIOD_TIME, self).__init__(format=format, **kwargs)
        self.frac_precision = frac_precision
        self.timezone       = timezone

class PERIOD_TIMESTAMP(_TDPeriod):

    """ Teradata PERIOD TIMESTAMP type

    This type identifies a field defining a duration with a beginning and end timestamp.

    """

    __visit_name__ = 'PERIOD_TIMESTAMP'

    def __init__(self, format=None, frac_precision=None, timezone=False, **kwargs):

        """ Construct a PERIOD_TIMESTAMP object

        :param format: format of the timestamp, e.g. 'YYYY-MM-DDBHH:MI:SS.S(6)'
        and 'YYYY-MM-DDBHH:MI:SS.S(6)Z' (with timezone)

        :param frac_precision: fractional_seconds_precision ranging from 0 to 6

        :param timezone: true if WITH TIME ZONE, false otherwise

        """
        super(PERIOD_TIMESTAMP, self).__init__(format=format, **kwargs)
        self.frac_precision = frac_precision
        self.timezone       = timezone


class CHAR(_TDConcatenable, _TDType, sqltypes.CHAR):

    """ Teradata CHAR type

    This type represents a fixed-length character string for Teradata Database
    internal character storage.

    """

    def __init__(self, length=1, charset=None, **kwargs):

        """ Construct a CHAR object

        :param length: number of characters or bytes allocated. Maximum value
        for n depends on the character set. For LATIN - 64000 characters,
        For UNICODE - 32000 characters, For KANJISJIS - 32000 bytes. If a value
        for n is not specified, the default is 1.

        :param charset: Server character set for the character column.
        Supported values:
            'LATIN': fixed 8-bit characters from the ASCII ISO 8859 Latin1
            or ISO 8859 Latin9.
            'UNICODE': fixed 16-bit characters from the UNICODE 6.0 standard.
            'GRAPHIC': fixed 16-bit UNICODE characters defined by IBM for DB2.
            'KANJISJIS': mixed single byte/multibyte characters intended for
            Japanese applications that rely on KanjiShiftJIS characteristics.
        Note: GRAPHIC(n) is equivalent to CHAR(n) CHARACTER SET GRAPHIC

        """
        super(CHAR, self).__init__(length=length, **kwargs)
        self.charset = charset


class VARCHAR(_TDConcatenable, _TDType, sqltypes.VARCHAR):

    """ Teradata VARCHAR type

    This type represents a variable length character string of length 0 to n
    for Teradata Database internal character storage. LONG VARCHAR specifies
    the longest permissible variable length character string for Teradata
    Database internal character storage.

    """

    def __init__(self, length=None, charset=None, **kwargs):

        """ Construct a VARCHAR object

        :param length: Optional 0 to n. If None, LONG is used
        (the longest permissible variable length character string)

        :param charset: optional character set for varchar.

        Note: VARGRAPHIC(n) is equivalent to VARCHAR(n) CHARACTER SET GRAPHIC

        """
        super(VARCHAR, self).__init__(length=length, **kwargs)
        self.charset = charset


class CLOB(_TDConcatenable, _TDType, sqltypes.CLOB):

    """ Teradata CLOB type

    This type represents a large character string. A character large object
    (CLOB) column can store character data, such as simple text or HTML.

    """

    def __init__(self, length=None, charset=None, multiplier=None, **kwargs):

        """ Construct a CLOB object

        :param length: Optional length for clob. For Latin server character set,
        length cannot exceed 2097088000. For Unicode server character set,
        length cannot exceed 1048544000.
        If no length is specified then the maximum is used.

        :param multiplier: Either 'K', 'M', or 'G'.
        K specifies number of characters to allocate as nK, where K=1024
        (For Latin char sets, n < 2047937 and For Unicode char sets, n < 1023968)
        M specifies nM, where M=1024K
        (For Latin char sets, n < 1999 and For Unicode char sets, n < 999)
        G specifies nG, where G=1024M
        (For Latin char sets, n must be 1 and char set must be LATIN)

        :param charset: LATIN (fixed 8-bit characters ASCII ISO 8859 Latin1 or ISO 8859 Latin9)
        or UNICODE (fixed 16-bit characters from the UNICODE 6.0 standard)

        """
        super(CLOB, self).__init__(length=length, **kwargs)
        self.charset    = charset
        self.multiplier = multiplier


class TeradataExpressionAdapter:
    """Expression Adapter for Teradata Data Types.

    For inferring the resulting type of a BinaryExpression whose operation
    involves operands that are of Teradata types.
    """

    def process(self, type_, op=None, other=None, **kw):
        """Adapts the expression.

        Infer the type of the resultant BinaryExpression defined by the passed
        in operator and operands. This resulting type should be consistent with
        the Teradata database when the operation is defined.

        Args:
            type_: The type instance of the left operand.

            op:    The operator of the BinaryExpression.

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
        """Adapt the expression according to some strategy.

        Given the type of the left and right operand, and the operator, produce
        a resulting type class for the BinaryExpression.
        """

        raise NotImplementedError()

class _IntervalRuleStrategy(_AdaptStrategy):
    """Expression adaptation strategy which follows a set of rules for inferring
    Teradata Interval types.
    """

    ordering = ('YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND')

    def adapt(self, type_, op, other, **kw):
        """Adapt the expression by a set of predefined rules over the Teradata
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
        elif isinstance(type_, _TDInterval) and isinstance(other, _TDInterval):
            tokens = self._tokenize_name(type_.__class__.__name__) + \
                     self._tokenize_name(other.__class__.__name__)
            tokens.sort(key=lambda tok: self.ordering.index(tok))

            return globals().get(
                self._combine_tokens(tokens[0], tokens[-1]),
                sqltypes.NullType)

        # Else the binary expression has an Interval and non-Interval operand.
        # If the non-Interval operand is a Date, Time, or Datetime, return that
        # type, otherwise return the Interval type.
        else:
            interval, non_interval = (type_, other) if \
                    isinstance(type_, _TDInterval) \
                else (other, type_)

            return non_interval.__class__ if \
                    isinstance(non_interval, (sqltypes.Date,
                                              sqltypes.Time,
                                              sqltypes.DateTime)) \
                else interval.__class__

    def _tokenize_name(self, interval_name):
        """Tokenize the name of Interval types.

        Returns a list of (str) tokens of the corresponding Interval type name.

        E.g. 'INTERVAL_DAY_TO_HOUR' --> ['DAY', 'HOUR'].
        """

        return list(filter(lambda tok: tok not in ('INTERVAL', 'TO'),
                           interval_name.split('_')))

    def _combine_tokens(self, tok_l, tok_r):
        """Combine the tokens of an Interval type to form its name.

        Returns a string for the name of the Interval type corresponding to the
        tokens passed in.

        E.g. tok_l='DAY' and tok_r='HOUR' --> 'INTERVAL_DAY_TO_HOUR'
        """

        return 'INTERVAL_%s_TO_%s' % (tok_l, tok_r)

class _LookupStrategy(_AdaptStrategy):
    """Expression adaptation strategy which employs a general lookup table."""

    def adapt(self, type_, op, other, **kw):
        """Adapt the expression by looking up a hardcoded table.

        The lookup table is defined as `visit_` methods below. Each method
        returns a nested dictionary which is keyed by the operator and the other
        operand's type.
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
                BIGINT:  BIGINT,
                DECIMAL: DECIMAL,
                FLOAT:   FLOAT,
                NUMBER:  NUMBER,
                DATE:    DATE
            },
            (operators.sub, operators.mul, operators.truediv,
             operators.mod): {
                (CHAR, VARCHAR, BLOB): FLOAT,
                BIGINT:  BIGINT,
                DECIMAL: DECIMAL,
                FLOAT:   FLOAT,
                NUMBER:  NUMBER
            }
        })

    def visit_SMALLINT(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.add: {
                (INTEGER, SMALLINT, BYTEINT, DATE): INTEGER,
                (CHAR, VARCHAR, BLOB): FLOAT,
                BIGINT:  BIGINT,
                DECIMAL: DECIMAL,
                FLOAT:   FLOAT,
                NUMBER:  NUMBER,
                DATE:    DATE
            },
            (operators.sub, operators.mul, operators.truediv,
             operators.mod): {
                (INTEGER, SMALLINT, BYTEINT, DATE): INTEGER,
                (CHAR, VARCHAR, BLOB): FLOAT,
                BIGINT:  BIGINT,
                DECIMAL: DECIMAL,
                FLOAT:   FLOAT,
                NUMBER:  NUMBER,
            }
        })

    def visit_BIGINT(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.add: {
                (CHAR, VARCHAR, BLOB): FLOAT,
                DECIMAL: DECIMAL,
                FLOAT:   FLOAT,
                NUMBER:  NUMBER,
                DATE:    DATE
            },
            (operators.sub, operators.mul, operators.truediv,
             operators.mod): {
                (CHAR, VARCHAR, BLOB): FLOAT,
                DECIMAL: DECIMAL,
                FLOAT:   FLOAT,
                NUMBER:  NUMBER
            }
        })

    def visit_DECIMAL(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.add: {
                (CHAR, VARCHAR, BLOB): FLOAT,
                FLOAT:  FLOAT,
                NUMBER: NUMBER,
                DATE:   DATE
            },
            (operators.sub, operators.mul, operators.truediv,
             operators.mod): {
                (CHAR, VARCHAR, BLOB): FLOAT,
                FLOAT:  FLOAT,
                NUMBER: NUMBER
            }
        })

    def visit_DATE(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.add: {
                (CHAR, VARCHAR, BLOB): FLOAT,
                DATE:  INTEGER,
                FLOAT: FLOAT
            },
            operators.sub: {
                (CHAR, VARCHAR, BLOB): FLOAT,
                DATE:  INTEGER,
                FLOAT: FLOAT
            },
            (operators.mul, operators.truediv, operators.mod): {
                (DATE, INTEGER, SMALLINT, BYTEINT): INTEGER,
                (CHAR, VARCHAR, BLOB): FLOAT,
                (FLOAT, TIME): FLOAT,
                BIGINT:  BIGINT,
                DECIMAL: DECIMAL,
                NUMBER:  NUMBER,
            }
        })

    def visit_TIME(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            (operators.add, operators.mul, operators.truediv,
             operators.mod): {
                DATE: FLOAT
            }
        })

    def visit_CHAR(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.concat_op: {
                CHAR:    VARCHAR if hasattr(other, 'charset') and \
                            ((type_.charset == 'unicode') !=
                             (other.charset == 'unicode'))
                         else CHAR,
                VARCHAR: VARCHAR,
                CLOB:    CLOB
            },
            (operators.add, operators.sub, operators.mul,
             operators.truediv, operators.mod): {
                (INTEGER, SMALLINT, BIGINT, BYTEINT, NUMBER, FLOAT, DECIMAL,
                 DATE, CHAR, VARCHAR): FLOAT
            }
        })

    def visit_VARCHAR(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.concat_op: {
                CLOB: CLOB
            },
            (operators.add, operators.sub, operators.mul,
             operators.truediv, operators.mod): {
                (INTEGER, SMALLINT, BIGINT, BYTEINT, NUMBER, FLOAT, DECIMAL,
                 DATE, CHAR, VARCHAR): FLOAT
            }
        })

    def visit_BYTEINT(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            (operators.add, operators.sub): {
                (INTEGER, SMALLINT, BYTEINT): INTEGER,
                (CHAR, VARCHAR, BLOB): FLOAT,
                BIGINT:   BIGINT,
                DECIMAL:  DECIMAL,
                FLOAT:    FLOAT,
                NUMBER:   NUMBER,
                DATE:     DATE
            },
            (operators.mul, operators.truediv, operators.mod): {
                (INTEGER, SMALLINT, BYTEINT, DATE): INTEGER,
                (CHAR, VARCHAR, BLOB): FLOAT,
                BIGINT:   BIGINT,
                DECIMAL:  DECIMAL,
                FLOAT:    FLOAT,
                NUMBER:   NUMBER
            }
        })

    def visit_BYTE(self, type_, other, **kw):
        return {
            operators.concat_op: {
                VARBYTE: VARBYTE,
                BLOB:    BLOB
            }
        }

    def visit_VARBYTE(self, type_, other, **kw):
        return {
            operators.concat_op: {
                BLOB: BLOB
            }
        }

    def visit_NUMBER(self, type_, other, **kw):
        return self._flatten_tuple_keyed_dict({
            operators.add: {
                (CHAR, VARCHAR, BLOB): FLOAT,
                FLOAT: FLOAT,
                DATE:  DATE
            },
            (operators.sub, operators.mul, operators.truediv,
             operators.mod): {
                (CHAR, VARCHAR, BLOB): FLOAT,
                FLOAT: FLOAT
            }
        })
