# sqlalchemy_teradata/types.py
# Copyright (C) 2015-2016 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy.sql import sqltypes
from sqlalchemy import types

import teradata.datatypes as td_dtypes
import datetime


class BYTEINT(sqltypes.Integer):
    """
    Teradata BYTEINT type.
    This type represents a one byte signed integer.
    """
    __visit_name__ = 'BYTEINT'


class DECIMAL(sqltypes.DECIMAL):

    """ Teradata Decimal/Numeric type """

    def __init__(self, precision=5, scale=0, **kw):
        """ Construct a Decimal type
        :param precision: max number of digits that can be stored (range from 1 thru 38)
        :param scale: number of fractional digits of :param precision: to the
                    right of the decimal point  (range from 0 to :param precision:)
        """
        super(DECIMAL, self).__init__(precision=precision, scale=scale, **kw)


class NUMERIC(sqltypes.NUMERIC):

    def __init__(self, precision=5, scale=0, **kw):
        super(NUMERIC, self).__init__(precision=precision, scale=scale, **kw)


class TIME(sqltypes.TIME):

    def __init__(self, precision=6, timezone=False, **kwargs):

        """ Construct a TIME stored as UTC in Teradata

        :param precision: optional fractional seconds precision. A single digit
        representing the number of significant digits in the fractional
        portion of the SECOND field. Valid values range from 0 to 6 inclusive.
        The default precision is 6

        :param timezone: If set to True creates a Time WITH TIME ZONE type

        """
        super(TIME, self).__init__(timezone=timezone, **kwargs)
        self.precision = precision


class TIMESTAMP(sqltypes.TIMESTAMP):

    def __init__(self, precision=6, timezone=False, **kwargs):
        """ Construct a TIMESTAMP stored as UTC in Teradata

        :param precision: optional fractional seconds precision. A single digit
        representing the number of significant digits in the fractional
        portion of the SECOND field. Valid values range from 0 to 6 inclusive.
        The default precision is 6

        :param timezone: If set to True creates a TIMESTAMP WITH TIME ZONE type

        """
        super(TIMESTAMP, self).__init__(timezone=timezone, **kwargs)
        self.precision = precision


class _TDInterval(types.UserDefinedType):

    """ Base class for the Teradata Interval sqltypes."""

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

    """ Teradata Interval Year data type
        Identifies a field defining a period of time in years

    """
    __visit_name__ = 'INTERVAL_YEAR'

    def __init__(self, precision=None, **kwargs):

       """
       precision: permitted range of digits for year ranging from 1 to 4

       """
       super(INTERVAL_YEAR, self).__init__(precision=precision)

class INTERVAL_YEAR_TO_MONTH(_TDInterval):

    """ Teradata Interval Year To Month data type
        Identifies a field defining a period of time in years and months
    """

    __visit_name__ = 'INTERVAL_YEAR_TO_MONTH'

    def __init__(self, precision=None, **kwargs):

        """
        precision: permitted range of digits for year ranging from 1 to 4

        """
        super(INTERVAL_YEAR_TO_MONTH, self).__init__(precision=precision)

class INTERVAL_MONTH(_TDInterval):

    """ Teradata Interval Month data type
        Identifies a field defining a period of time in months
    """

    __visit_name__ = 'INTERVAL_MONTH'

    def __init__(self, precision=None, **kwargs):

        """
        precision: permitted range of digits for month ranging from 1 to 4

        """
        super(INTERVAL_MONTH, self).__init__(precision=precision)

class INTERVAL_DAY(_TDInterval):

    """ Teradata Interval Day data type
        Identifies a field defining a period of time in days
    """

    __visit_name__ = 'INTERVAL_DAY'

    def __init__(self, precision=None, **kwargs):

        """
        precision: permitted range of digits for day ranging from 1 to 4

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

    """ Teradata Interval Day To Hour data type
        Identifies a field defining a period of time in days and hours
    """

    __visit_name__ = 'INTERVAL_DAY_TO_HOUR'

    def __init__(self, precision=None, **kwargs):

        """
        precision: permitted range of digits for day ranging from 1 to 4

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

    """ Teradata Interval Day To Minute data type
        Identifies a field defining a period of time in days, hours, and minutes
    """

    __visit_name__= 'INTERVAL_DAY_TO_MINUTE'

    def __init__(self, precision=None, **kwargs):

        """
        precision: permitted range of digits for day ranging from 1 to 4

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

    """ Teradata Interval Day To Second data type
        Identifies a field during a period of time in days, hours, minutes, and seconds
    """

    __visit_name__='INTERVAL_DAY_TO_SECOND'

    def __init__(self, precision=None, frac_precision=None, **kwargs):

        """
        precision: permitted range of digits for day ranging from 1 to 4
        frac_precision: fracional_seconds_precision ranging from 0 to 6

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

    """ Teradata Interval Hour data type
        Identifies a field defining a period of time in hours
    """

    __visit_name__='INTERVAL_HOUR'

    def __init__(self, precision=None, **kwargs):

        """
        precision: permitted range of digits for hour ranging from 1 to 4

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

    """ Teradata Interval Hour To Minute data type
        Identifies a field defining a period of time in hours and minutes
    """

    __visit_name__='INTERVAL_HOUR_TO_MINUTE'

    def __init__(self, precision=None, **kwargs):

        """
        precision: permitted range of digits for hour ranging from 1 to 4

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

    """ Teradata Interval Hour To Second data type
        Identifies a field defining a period of time in hours, minutes, and seconds
    """

    __visit_name__='INTERVAL_HOUR_TO_SECOND'

    def __init__(self, precision=None, frac_precision=None, **kwargs):

        """
        precision: permitted range of digits for hour ranging from 1 to 4
        frac_precision: fracional_seconds_precision ranging from 0 to 6

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

    """ Teradata Interval Minute type
        Identifies a field defining a period of time in minutes
    """

    __visit_name__='INTERVAL_MINUTE'

    def __init__(self, precision=None, **kwargs):

        """
        precision: permitted range of digits for minute ranging from 1 to 4

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

    """ Teradata Interval Minute To Second data type
        Identifies a field defining a period of time in minutes and seconds
    """

    __visit_name__='INTERVAL_MINUTE_TO_SECOND'

    def __init__(self, precision=None, frac_precision=None, **kwargs):

        """
        precision: permitted range of digits for minute ranging from 1 to 4
        frac_precision: fracional_seconds_precision ranging from 0 to 6

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

    """ Teradata Interval Second data type
        Identifies a field defining a period of time in seconds
    """

    __visit_name__ = 'INTERVAL_SECOND'

    def __init__(self, precision=None, frac_precision=None, **kwargs):

        """
        precision: permitted range of digits for second ranging from 1 to 4
        frac_precision: fractional_seconds_precision ranging from 0 to 6

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

    """ Base class for the Teradata Period sqltypes."""

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

    """ Teradata Period Date data type
        Identifies a field defining a duration with a beginning and end date
    """

    __visit_name__ = 'PERIOD_DATE'

    def __init__(self, format=None, **kwargs):

        """
        format: format of the date, e.g. 'yyyy-mm-dd'

        """
        super(PERIOD_DATE, self).__init__(format=format, **kwargs)

class PERIOD_TIME(_TDPeriod):

    """ Teradata Period Time data type
        Identifies a field defining a duration with a beginning and end time
    """

    __visit_name__ = 'PERIOD_TIME'

    def __init__(self, format=None, frac_precision=None, timezone=False, **kwargs):

        """
        format:         format of the time, e.g. 'HH:MI:SS.S(6)' and
                        'HH:MI:SS.S(6)Z' (with timezone)
        frac_precision: fractional_seconds_precision ranging from 0 to 6
        timezone:       true if WITH TIME ZONE, false otherwise

        """
        super(PERIOD_TIME, self).__init__(format=format, **kwargs)
        self.frac_precision = frac_precision
        self.timezone       = timezone

class PERIOD_TIMESTAMP(_TDPeriod):

    """ Teradata Period Timestamp data type
        Identifies a field defining a duration with a beginning and end timestamp
    """

    __visit_name__ = 'PERIOD_TIMESTAMP'

    def __init__(self, format=None, frac_precision=None, timezone=False, **kwargs):

        """
        format:         format of the timestamp, e.g. 'YYYY-MM-DDBHH:MI:SS.S(6)'
                        and 'YYYY-MM-DDBHH:MI:SS.S(6)Z' (with timezone)
        frac_precision: fractional_seconds_precision ranging from 0 to 6
        timezone:       true if WITH TIME ZONE, false otherwise

        """
        super(PERIOD_TIMESTAMP, self).__init__(format=format, **kwargs)
        self.frac_precision = frac_precision
        self.timezone       = timezone


class CHAR(sqltypes.CHAR):

    def __init__(self, length=1, charset=None, **kwargs):

        """ Construct a Char

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

    def __init__(self, length=None, charset=None, **kwargs):

        """Construct a Varchar

        :param length: Optional 0 to n. If None, LONG is used
        (the longest permissible variable length character string)

        :param charset: optional character set for varchar.

        Note: VARGRAPHIC(n) is equivalent to VARCHAR(n) CHARACTER SET GRAPHIC

        """
        super(VARCHAR, self).__init__(length=length, **kwargs)
        self.charset = charset


class CLOB(sqltypes.CLOB):

    def __init__(self, length=None, charset=None, multiplier=None, **kwargs):

        """Construct a Clob

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
        self.charset = charset
        self.multiplier=multiplier


class BLOB(sqltypes.LargeBinary):
        pass
