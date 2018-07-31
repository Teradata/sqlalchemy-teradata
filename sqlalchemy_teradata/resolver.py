class TeradataTypeResolver:

    def process(self, type_, **kw):
        return type_._compiler_dispatch(type_, self, **kw)


    def visit_INTEGER(self, type_, **kw):
        return type_()

    def visit_SMALLINT(self, type_, **kw):
        return type_()

    def visit_BIGINT(self, type_, **kw):
        return type_()

    def visit_DECIMAL(self, type_, **kw):
        return type_(precision=kw['prec'], scale=kw['scale'])

    def visit_DATE(self, type_, **kw):
        return type_()


    def _resolve_type_interval(self, type_, **kw):
        return type_(precision=kw['prec'], frac_precision=kw['scale'])

    def visit_INTERVAL_YEAR(self, type_, **kw):
        return self._resolve_type_interval(type_, **kw)

    def visit_INTERVAL_YEAR_TO_MONTH(self, type_, **kw):
        return self._resolve_type_interval(type_, **kw)

    def visit_INTERVAL_MONTH(self, type_, **kw):
        return self._resolve_type_interval(type_, **kw)

    def visit_INTERVAL_DAY(self, type_, **kw):
        return self._resolve_type_interval(type_, **kw)

    def visit_INTERVAL_DAY_TO_HOUR(self, type_, **kw):
        return self._resolve_type_interval(type_, **kw)

    def visit_INTERVAL_DAY_TO_MINUTE(self, type_, **kw):
        return self._resolve_type_interval(type_, **kw)

    def visit_INTERVAL_DAY_TO_SECOND(self, type_, **kw):
        return self._resolve_type_interval(type_, **kw)

    def visit_INTERVAL_HOUR(self, type_, **kw):
        return self._resolve_type_interval(type_, **kw)

    def visit_INTERVAL_HOUR_TO_MINUTE(self, type_, **kw):
        return self._resolve_type_interval(type_, **kw)

    def visit_INTERVAL_HOUR_TO_SECOND(self, type_, **kw):
        return self._resolve_type_interval(type_, **kw)

    def visit_INTERVAL_MINUTE(self, type_, **kw):
        return self._resolve_type_interval(type_, **kw)

    def visit_INTERVAL_MINUTE_TO_SECOND(self, type_, **kw):
        return self._resolve_type_interval(type_, **kw)

    def visit_INTERVAL_SECOND(self, type_, **kw):
        return self._resolve_type_interval(type_, **kw)


    def visit_PERIOD_DATE(self, type_, **kw):
        return type_(format=kw['fmt'])

    def _resolve_type_period_datetime(self, type_, **kw):
        tz = kw['tc'] == 'pz' or kw['tc'] == 'pm'
        return type_(format=kw['fmt'], frac_precision=kw['scale'], timezone=tz)

    def visit_PERIOD_TIME(self, type_, **kw):
        return self._resolve_type_period_datetime(type_, **kw)

    def visit_PERIOD_TIMESTAMP(self, type_, **kw):
        return self._resolve_type_period_datetime(type_, **kw)


    def _resolve_type_datetime(self, type_, **kw):
        tz = kw['fmt'][-1] == 'Z'
        prec = kw['fmt']
        prec = prec[prec.index('(') + 1: prec.index(')')] if '(' in prec else 0
        prec = kw['scale'] if prec == 'F' else int(prec)
        return type_(precision=prec, timezone=tz)

    def visit_TIME(self, type_, **kw):
        return self._resolve_type_datetime(type_, **kw)

    def visit_TIMESTAMP(self, type_, **kw):
        return self._resolve_type_datetime(type_, **kw)


    def _resolve_type_string(self, type_, **kw):
        return type_(
            length=int(kw['length'] / 2) if
                    (kw['chartype'] == 'UNICODE' or
                     kw['chartype'] == 'GRAPHIC')
                    else kw['length'],
            charset=kw['chartype'])

    def visit_CHAR(self, type_, **kw):
        return self._resolve_type_string(type_, **kw)

    def visit_VARCHAR(self, type_, **kw):
        return self._resolve_type_string(type_, **kw)

    def visit_CLOB(self, type_, **kw):
        return self._resolve_type_string(type_, **kw)


    def visit_BYTEINT(self, type_, **kw):
        return type_()

    def visit_FLOAT(self, type_, **kw):
        return type_()


    def _resolve_type_binary(self, type_, **kw):
        return type_(length=kw['length'])

    def visit_BYTE(self, type_, **kw):
        return self._resolve_type_binary(type_, **kw)

    def visit_VARBYTE(self, type_, **kw):
        return self._resolve_type_binary(type_, **kw)

    def visit_BLOB(self, type_, **kw):
        return self._resolve_type_binary(type_, **kw)


    def visit_NUMBER(self, type_, **kw):
        return type_(precision=kw['prec'], scale=kw['scale'])
