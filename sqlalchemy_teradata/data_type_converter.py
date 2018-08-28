# sqlalchemy_teradata/data_type_converter.py
# Copyright (C) 2015-2019 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from teradata.datatypes import DefaultDataTypeConverter

class TDDataTypeConverter(DefaultDataTypeConverter):

    def __init__(self, *args, **kwargs):
        super(TDDataTypeConverter, self).__init__(*args, **kwargs)

    def _process_data_type(self, dataType):
        if 'INTERVAL' in dataType:
            return dataType.replace('_', ' ')
        return dataType

    def convertValue(self, dbType, dataType, typeCode, value):
        dataType = self._process_data_type(dataType)
        return super(TDDataTypeConverter, self).convertValue(
            dbType, dataType, typeCode, value)
