# sqlalchemy_teradata/__init__.py
# Copyright (C) 2015-2016 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from .types import TIME, TIMESTAMP, DECIMAL, CHAR, VARCHAR, CLOB, BYTEINT
from sqlalchemy.sql.sqltypes import (Integer, Interval, SmallInteger,\
                                     BigInteger, Float, Boolean,\
                                     Text, Unicode, UnicodeText,\
                                     DATE)
__version__ = '0.1.0'

__all__ = (Integer, SmallInteger, BigInteger, Float, Text, Unicode,
           UnicodeText, Interval, Boolean,
           DATE, TIME, TIMESTAMP, DECIMAL,
           CHAR, VARCHAR, CLOB, BYTEINT)

from teradata import tdodbc
