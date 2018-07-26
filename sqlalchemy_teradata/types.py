# sqlalchemy_teradata/types.py
# Copyright (C) 2015-2016 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy import types
from sqlalchemy.sql import sqltypes

import datetime
import teradata.datatypes as td_dtypes


class BYTEINT(sqltypes.Integer):

    """ Teradata BYTEINT type

    This type represents a one byte signed integer.

    """

    __visit_name__ = 'BYTEINT'

    def __init__(self, **kwargs):

        """ Construct a BYTEINT Object """
        super(BYTEINT, self).__init__(**kwargs)


class BYTE(sqltypes.BINARY):

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


class VARBYTE(sqltypes.VARBINARY):

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


class BLOB(sqltypes.LargeBinary):

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


class NUMBER(sqltypes.NUMERIC):

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


class TIME(sqltypes.TIME):

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


class TIMESTAMP(sqltypes.TIMESTAMP):

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


class _TDInterval(types.UserDefinedType):

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


class _TDPeriod(types.UserDefinedType):

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


class CHAR(sqltypes.CHAR):

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


class VARCHAR(sqltypes.String):

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


class CLOB(sqltypes.CLOB):

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
