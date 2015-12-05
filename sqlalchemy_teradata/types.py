from sqlalchemy import types

class TeradataDate(types.Date):
    def get_col_spec(self):
        return "DATETIME"

class TeradataDateTime(types.DateTime):
    def get_col_spec(self):
        return "DATETIME"

class TeradataTime(types.Time):
    def get_col_spec(self):
        return "TIME"

