# sqlalchemy_teradata/types.py
# Copyright (C) 2015-2016 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy.sql import sqltypes

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


class _TDInterval(sqltypes.Interval):

    """ Base class for teradata interval sqltypes."""

    def __init__(self, precision=None, frac_precision=None, **kwargs):

        self.precision = precision
        self.frac_precision = frac_precision

        super(_TDInterval, self).__init__(**kwargs)


class IntervalYear(_TDInterval, sqltypes.Interval):

    """ Teradata Interval Year data type
        Identifies a field defining a period of time in years

    """
    __visit_name__ = 'interval_year'

    def __init__(self, precision=None, **kwargs):

       """
       precision: permitted range of digits for year ranging from 1 to 4

       """
       return super(IntervalYear, self).__init__(precision=precision, **kwargs)

class IntervalYearToMonth(_TDInterval, sqltypes.Interval):

   """ Teradata Interval Year To Month data type
       Identifies a field defining a period of time in years and months
   """

   __visit_name__ = 'interval_year_to_month'

   def __init__(self, precision=None, **kwargs):

       """
        precision: permitted range of digits for year ranging from 1 to 4

       """
       return super(IntervalYearToMonth, self).__init__(precision=precision, **kwargs)

class IntervalMonth(_TDInterval, sqltypes.Interval):

   """ Teradata Interval Month data type
       Identifies a field defining a period of time in months
   """

   __visit_name__ = 'interval_month'

   def __init__(self, precision=None, **kwargs):

       """
         precision: permitted range of digits for month ranging from 1 to 4

       """
       return super(IntervalMonth, self).__init__(precision=precision, **kwargs)

class IntervalDay(_TDInterval, sqltypes.Interval):

   """ Teradata Interval Day data type
       Identifies a field defining a period of time in days
   """

   __visit_name__ = 'interval_day'

   def __init__(self, precision=None, **kwargs):

       """
         precision: permitted range of digits for day ranging from 1 to 4

       """
       return super(IntervalDay, self).__init__(precision=precision, **kwargs)


class IntervalDayToHour(_TDInterval, sqltypes.Interval):

   """ Teradata Interval Day To Hour data type
       Identifies a field defining a period of time in days and hours
   """
   __visit_name__ = 'interval_day_to_hour'

   def __init__(self, precision=None, **kwargs):

       """
         precision: permitted range of digits for day ranging from 1 to 4

       """

       return super(IntervalDayToHour, self).__init__(precision=precision, **kwargs)

class IntervalDayToMinute(_TDInterval, sqltypes.Interval):

   """ Teradata Interval Day To Minute data type
       Identifies a field defining a period of time in days, hours, and minutes
   """

   __visit_name__= 'interval_day_to_minute'

   def __init__(self, precision=None, **kwargs):

       """
        precision: permitted range of digits for day ranging from 1 to 4

       """

       return super(IntervalDayToMinute, self).__init__(precision=precision, **kwargs)

class IntervalDayToSecond(_TDInterval, sqltypes.Interval):

   """ Teradata Interval Day To Second data type
       Identifies a field during a period of time in days, hours, minutes, and seconds
   """

   __visit_name__='interval_day_to_second'

   def __init__(self, precision=None, frac_precision=None, **kwargs):

       """
         precision: permitted range of digits for day ranging from 1 to 4
         frac_precision: fracional_seconds_precision ranging from 0 to 6

       """
       return super(IntervalDayToSecond, self).__init__(precision=precision,
                                                         frac_precision=frac_precision,
                                                         **kwargs)

class IntervalHour(_TDInterval, sqltypes.Interval):

   """ Teradata Interval Hour data type
       Identifies a field defining a period of time in hours
   """

   __visit_name__='interval_hour'

   def __init__(self, precision=None, **kwargs):

       """
         precision: permitted range of digits for hour ranging from 1 to 4

       """
       return super(IntervalHour, self).__init__(precision=precision, **kwargs)

class IntervalHourToMinute(_TDInterval, sqltypes.Interval):

   """ Teradata Interval Hour To Minute data type
       Identifies a field defining a period of time in hours and minutes
   """

   __visit_name__='interval_hour_to_minute'

   def __init__(self, precision=None, **kwargs):

       """
         precision: permitted range of digits for month ranging from 1 to 4

       """
       return super(IntervalHourToMinute, self).__init__(precision=precision,
                                                          **kwargs)

class IntervalHourToSecond(_TDInterval, sqltypes.Interval):

   """ Teradata Interval Hour To Second data type
       Identifies a field defining a period of time in hours, minutes, and seconds
   """

   __visit_name__='interval_hour_to_second'

   def __init__(self, precision=None, frac_precision=None, **kwargs):

       """
         precision: permitted range of digits for hour ranging from 1 to 4
         frac_precision: fracional_seconds_precision ranging from 0 to 6

       """
       return super(IntervalHourToSecond, self).__init__(precision=precision,
                                                        frac_precision=frac_precision,
                                                        **kwargs)

class IntervalMinute(_TDInterval, sqltypes.Interval):

   """ Teradata Interval Minute type
       Identifies a field defining a period of time in minutes
   """

   __visit_name__='interval_minute'

   def __init__(self, precision=None, **kwargs):

       """
         precision: permitted range of digits for minute ranging from 1 to 4

       """
       return super(IntervalMinute, self).__init__(precision=precision, **kwargs)

class IntervalMinuteToSecond(_TDInterval, sqltypes.Interval):

   """ Teradata Interval Minute To Second data type
       Identifies a field defining a period of time in minutes and seconds
   """

   __visit_name__='interval_minute_to_second'

   def __init__(self, precision=None, frac_precision=None, **kwargs):

       """
         precision: permitted range of digits for minute ranging from 1 to 4
         frac_precision: fracional_seconds_precision ranging from 0 to 6

       """
       return super(IntervalMinuteToSecond, self).__init__(precision=precision,
                                                          frac_precision=frac_precision,
                                                          **kwargs)

class IntervalSecond(_TDInterval, sqltypes.Interval):

   """ Teradata Interval Second data type
       Identifies a field defining a period of time in seconds
   """

   __visit_name__ = 'interval_second'

   def __init__(self, precision=None, frac_precision=None, **kwargs):

       """
        precision: permitted range of digits for second ranging from 1 to 4
        frac_precision: fractional_seconds_precision ranging from 0 to 6

       """
       return super(IntervalSecond, self).__init__(precision=precision, 
                                                  frac_precision=frac_precision, **kwargs)

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

        :param length: Optional 0 to n. If None, it LONG is used
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
