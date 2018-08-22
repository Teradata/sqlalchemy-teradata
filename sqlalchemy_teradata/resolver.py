class TeradataTypeResolver:
    """Type Resolver for Teradata Data Types.

    For dynamically instantiating instances of TypeEngine (subclasses).
    This class mimics the design of SQLAlchemy's TypeCompiler and in fact
    takes advantage of the compiler's visitor double-dispatch mechanism.
    This is accomplished by having the main process method redirect to the
    passed in type_'s corresponding visit method defined by the TypeResolver
    below.
    """

    def process(self, type_, **kw):
        """Resolves the type.

        Instantiate the type and populate its relevant attributes with the
        appropriate keyword arguments.

        Args:
            type_: The type to be resolved (instantiated).

            **kw:  Keyword arguments used for populating the attributes of the
                   type being resolved.

        Returns:
            An instance of type_ correctly populated with the appropriate
            keyword arguments.
        """

        return getattr(self, 'visit_' + type_.__visit_name__)(type_, **kw)

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

    def visit_PERIOD_TIME(self, type_, **kw):
        tz = kw['typecode'] == 'PZ'
        return type_(format=kw['fmt'], frac_precision=kw['scale'], timezone=tz)

    def visit_PERIOD_TIMESTAMP(self, type_, **kw):
        tz = kw['typecode'] == 'PM'
        return type_(format=kw['fmt'], frac_precision=kw['scale'], timezone=tz)

    def visit_TIME(self, type_, **kw):
        tz = kw['typecode'] == 'TZ'
        return type_(precision=kw['scale'], timezone=tz)

    def visit_TIMESTAMP(self, type_, **kw):
        tz = kw['typecode'] == 'SZ'
        return type_(precision=kw['scale'], timezone=tz)

    def _resolve_type_string(self, type_, **kw):
        return type_(
            length=int(kw['length'] / 2) if
                   (kw['chartype'] == 'UNICODE' or kw['chartype'] == 'GRAPHIC')
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
        # TODO Multiplier of BLOB currently not recovered when reflected
        return self._resolve_type_binary(type_, **kw)

    def visit_NUMBER(self, type_, **kw):
        return type_(precision=kw['prec'], scale=kw['scale'])
