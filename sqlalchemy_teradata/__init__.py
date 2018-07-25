# sqlalchemy_teradata/__init__.py
# Copyright (C) 2015-2016 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .base  import (INTEGER, SMALLINT, BIGINT, DECIMAL, FLOAT, DATE)
from .types import (TIME, TIMESTAMP, CHAR, VARCHAR, CLOB, NUMBER, BYTEINT,
                    BYTE, VARBYTE, BLOB, INTERVAL_YEAR, INTERVAL_YEAR_TO_MONTH,
                    INTERVAL_MONTH, INTERVAL_DAY, INTERVAL_DAY_TO_HOUR,
                    INTERVAL_DAY_TO_MINUTE, INTERVAL_DAY_TO_SECOND, INTERVAL_HOUR,
                    INTERVAL_HOUR_TO_MINUTE, INTERVAL_HOUR_TO_SECOND,
                    INTERVAL_MINUTE, INTERVAL_MINUTE_TO_SECOND, INTERVAL_SECOND,
                    PERIOD_DATE, PERIOD_TIME, PERIOD_TIMESTAMP)

__version__ = '0.1.0'

__all__ = ('INTEGER', 'SMALLINT', 'BIGINT', 'DECIMAL', 'FLOAT', 'DATE', 'TIME',
           'TIMESTAMP', 'CHAR', 'VARCHAR', 'CLOB', 'NUMBER', 'BYTEINT', 'BYTE',
           'VARBYTE', 'BLOB', 'INTERVAL_YEAR', 'INTERVAL_YEAR_TO_MONTH',
           'INTERVAL_MONTH', 'INTERVAL_DAY', 'INTERVAL_DAY_TO_HOUR',
           'INTERVAL_DAY_TO_MINUTE', 'INTERVAL_DAY_TO_SECOND', 'INTERVAL_HOUR',
           'INTERVAL_HOUR_TO_MINUTE', 'INTERVAL_HOUR_TO_SECOND', 'INTERVAL_MINUTE',
           'INTERVAL_MINUTE_TO_SECOND', 'INTERVAL_SECOND', 'PERIOD_DATE',
           'PERIOD_TIME', 'PERIOD_TIMESTAMP')

from teradata import tdodbc

# - BigInteger (BIGINT)
# - Boolean (BYTEINT, w/ contraint in (0, 1))
# - Date (DATE)
# - DateTime (TIMESTAMP)
# - Enum (VARCHAR, w/ constraint in set of enumerated strings)
# - Float (FLOAT)
# - Integer (INTEGER)
# - Interval (TIMESTAMP)
# - LargeBinary (BLOB)
# - Numeric (NUMERIC/FLOAT)
# - SmallInteger (SMALLINT)
# - String (LONG VARCHAR)
# - Text (CLOB)
# - Time (TIME)
# - Unicode (LONG VARCHAR CHAR SET UNICODE)
# - UnicodeText (CLOB CHAR SET UNICODE)
