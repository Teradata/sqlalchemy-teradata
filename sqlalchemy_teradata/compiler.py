# sqlalchemy_teradata/compiler.py
# Copyright (C) 2015-2016 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy.sql import compiler
from sqlalchemy import exc
from sqlalchemy import schema as sa_schema
from sqlalchemy.types import Unicode
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Select
from sqlalchemy.sql import operators
from sqlalchemy import exc, sql
from sqlalchemy import create_engine


class TeradataCompiler(compiler.SQLCompiler):

    def __init__(self, dialect, statement, column_keys=None, inline=False, **kwargs):
        super(TeradataCompiler, self).__init__(dialect, statement, column_keys, inline, **kwargs)

    def get_select_precolumns(self, select, **kwargs):
        """
        handles the part of the select statement before the columns are specified.
        Note: Teradata does not allow a 'distinct' to be specified when 'top' is
              used in the same select statement.

              Instead if a user specifies both in the same select clause,
              the DISTINCT will be used with a ROW_NUMBER OVER(ORDER BY) subquery.
        """

        pre = select._distinct and "DISTINCT " or ""

        #TODO: decide whether we can replace this with the recipe...
        if (select._limit is not None and select._offset is None):
            pre += "TOP %d " % (select._limit)

        return pre

    def visit_binary(self, binary, override_operator=None,
                     eager_grouping=False, **kw):
        """
        Handles the overriding of binary operators. Any custom binary operator
        definition should be placed in the custom_ops dict.
        """
        custom_ops = {
            operators.ne:  '<>',
            operators.mod: 'MOD'
        }

        if binary.operator in custom_ops:
            binary.operator = operators.custom_op(
                opstring=custom_ops[binary.operator])

        return compiler.SQLCompiler.visit_binary(
            self, binary, override_operator, eager_grouping, **kw)

    def limit_clause(self, select, **kwargs):
        """Limit after SELECT is implemented in get_select_precolumns"""
        return ""

class TeradataDDLCompiler(compiler.DDLCompiler):

    def visit_create_index(self, create, include_schema=False,
                           include_table_schema=True):
        index = create.element
        self._verify_index_table(index)
        preparer = self.preparer
        text = "CREATE "
        if index.unique:
            text += "UNIQUE "
        text += "INDEX %s (%s) ON %s" \
            % (
                self._prepared_index_name(index,
                                          include_schema=include_schema),
                ', '.join(
                    self.sql_compiler.process(
                        expr, include_table=False, literal_binds=True) for
                    expr in index.expressions),
                preparer.format_table(index.table,
                                      use_schema=include_table_schema)
            )
        return text

    def create_table_suffix(self, table):
        """
        This hook processes the optional keyword teradata_suffixes
        ex.
        from sqlalchemy_teradata.compiler import\
                        TDCreateTableSuffix as Opts
        t = Table( 'name', meta,
                   ...,
                   teradata_suffixes=Opts.
                                      fallback().
                                      log().
                                      with_journal_table(t2.name)

        CREATE TABLE name, fallback,
        log,
        with journal table = [database/user.]table_name(
          ...
        )

        teradata_suffixes can also be a list of strings to be appended
        in the order given.
        """
        post=table.dialect_kwargs['teradata_suffixes']

        if isinstance(post, TDCreateTableSuffix):
            if post.opts:
                return ',\n' + post.compile()
            else:
                return post
        elif post:
            assert type(post) is list
            res = ',\n ' + ',\n'.join(post)
        else:
            return ''

    def post_create_table(self, table):

        """
        This hook processes the TDPostCreateTableOpts given by the
        teradata_post_create dialect kwarg for Table.

        Note that there are other dialect kwargs defined that could possibly
        be processed here.

        See the kwargs defined in dialect.TeradataDialect

        Ex.
        from sqlalchemy_teradata.compiler import TDCreateTablePost as post
        Table('t1', meta,
               ...
               ,
               teradata_post_create = post().
                                        fallback().
                                        checksum('on').
                                        mergeblockratio(85)

        creates ddl for a table like so:

        CREATE TABLE "t1" ,
             checksum=on,
             fallback,
             mergeblockratio=85 (
               ...
        )

        """
        kw = table.dialect_kwargs['teradata_post_create']
        if isinstance(kw, TDCreateTablePost):
            if kw:
              return '\n' + kw.compile()
        return ''

    def get_column_specification(self, column, **kwargs):

        if column.table is None:
            raise exc.CompileError(
                "Teradata requires Table-bound columns "
                "in order to generate DDL")

        colspec = (self.preparer.format_column(column) + " " +\
                        self.dialect.type_compiler.process(
                          column.type, type_expression=column))

        # Null/NotNull
        if column.nullable is not None:
            if not column.nullable or column.primary_key:
                colspec += " NOT NULL"

        return colspec

class TeradataOptions(object):
    """
    An abstract base class for various schema object options
    """
    def _append(self, opts, val):
        _opts=opts.copy()
        _opts.update(val)
        return _opts

    def compile(self):
        """
        processes the argument options and returns a string representation
        """
        pass

    def format_cols(self, key, val):

        """
        key is a string
        val is a list of strings with an optional dict as the last element
            the dict values are appended at the end of the col list
        """
        res = ''
        col_expr = ', '.join([x for x in val if type(x) is str])

        res += key + '( ' + col_expr + ' )'
        if type(val[-1]) is dict:
            # process syntax elements (dict) after cols
            res += ' '.join( val[-1]['post'] )
        return res

class TDCreateTableSuffix(TeradataOptions):
    """
    A generative class for Teradata create table options
    specified in teradata_suffixes
    """
    def __init__(self, opts={}):
        """
        opts is a dictionary that can be pre-populated with key-value pairs
        that may be overidden if the keys conflict with those entered
        in the methods below. See the compile method to see how the dict
        gets processed.
        """
        self.opts = opts

    def compile(self):
        def process_opts(opts):
            return [key if opts[key] is None else '{}={}'.\
                            format(key, opts[key]) for key in opts]

        res = ',\n'.join(process_opts(self.opts))
        return res

    def fallback(self, enabled=True):
        res = 'fallback' if enabled else 'no fallback'
        return self.__class__(self._append(self.opts, {res:None}))

    def log(self, enabled=True):
        res = 'log' if enabled else 'no log'
        return self.__class__(self._append(self.opts, {res:None}))

    def with_journal_table(self, tablename=None):
        """
        tablename is the schema.tablename of a table.
        For example, if t1 is a SQLAlchemy:
                with_journal_table(t1.name)
        """
        return self.__class__(self._append(self.opts,\
                        {'with journal table':tablename}))

    def before_journal(self, prefix='dual'):
        """
        prefix is a string taking vaues of 'no' or 'dual'
        """
        assert prefix in ('no', 'dual')
        res = prefix+' '+'before journal'
        return self.__class__(self._append(self.opts, {res:None}))

    def after_journal(self, prefix='not local'):
        """
        prefix is a string taking vaues of 'no', 'dual', 'local',
        or 'not local'.
        """
        assert prefix in ('no', 'dual', 'local', 'not local')
        res = prefix+' '+'after journal'
        return self.__class__(self._append(self.opts, {res:None}))

    def checksum(self, integrity_checking='default'):
        """
        integrity_checking is a string taking vaues of 'on', 'off',
        or 'default'.
        """
        assert integrity_checking in ('on', 'off', 'default')
        return self.__class__(self._append(self.opts,\
                        {'checksum':integrity_checking}))

    def freespace(self, percentage=0):
        """
        percentage is an integer taking values from 0 to 75.
        """
        return self.__class__(self._append(self.opts,\
                        {'freespace':percentage}))

    def no_mergeblockratio(self):
        return self.__class__(self._append(self.opts,\
                        {'no mergeblockratio':None}))

    def mergeblockratio(self, integer=None):
        """
        integer takes values from 0 to 100 inclusive.
        """
        res = 'default mergeblockratio' if integer is None\
                                        else 'mergeblockratio'
        return self.__class__(self._append(self.opts, {res:integer}))

    def min_datablocksize(self):
            return self.__class__(self._append(self.opts,\
                            {'minimum datablocksize':None}))

    def max_datablocksize(self):
        return self.__class__(self._append(self.opts,\
                        {'maximum datablocksize':None}))

    def datablocksize(self, data_block_size=None):
        """
        data_block_size is an integer specifying the number of bytes
        """
        res = 'datablocksize' if data_block_size is not None\
                              else 'default datablocksize'
        return self.__class__(self._append(self.opts,\
                                           {res:data_block_size}))

    def blockcompression(self, opt='default'):
        """
        opt is a string that takes values 'autotemp',
        'default', 'manual', or 'never'
        """
        return self.__class__(self._append(self.opts,\
                        {'blockcompression':opt}))

    def with_no_isolated_loading(self, concurrent=False):
        res = 'with no ' +\
            ('concurrent ' if concurrent else '') +\
            'isolated loading'
        return self.__class__(self._append(self.opts, {res:None}))

    def with_isolated_loading(self, concurrent=False, opt=None):
        """
        opt is a string that takes values 'all', 'insert', 'none',
        or None
        """
        assert opt in ('all', 'insert', 'none', None)
        for_stmt = ' for ' + opt if opt is not None else ''
        res = 'with ' +\
            ('concurrent ' if concurrent else '') +\
            'isolated loading' + for_stmt
        return self.__class__(self._append(self.opts, {res:None}))


class TDCreateTablePost(TeradataOptions):
    """
    A generative class for building post create table options
    given in the teradata_post_create keyword for Table
    """
    def __init__(self, opts={}):
        self.opts = opts

    def compile(self):
        def process(opts):
            return [key.upper() if opts[key] is None\
                       else self.format_cols(key, opts[key])\
                       for key in opts]

        return ',\n'.join(process(self.opts))

    def no_primary_index(self):
        return self.__class__(self._append(self.opts, {'no primary index':None}))

    def primary_index(self, name=None, unique=False, cols=[]):
        """
        name is a string for the primary index
        if unique is true then unique primary index is specified
        cols is a list of column names
        """
        res = 'unique primary index' if unique else 'primary index'
        res += ' ' + name if name is not None else ''
        return self.__class__(self._append(self.opts, {res:cols}))


    def primary_amp(self, name=None, cols=[]):

        """
        name is an optional string for the name of the amp index
        cols is a list of column names (strings)
        """
        res = 'primary amp index'
        res += ' ' + name if name is not None else ''
        return self.__class__(self._append(self.opts, {res:cols}))


    def partition_by_col(self, all_but=False, cols={}, rows={}, const=None):

        """
        ex:

        Opts.partition_by_col(cols ={'c1': True, 'c2': False, 'c3': None},
                     rows ={'d1': True, 'd2':False, 'd3': None},
                     const = 1)
        will emit:

        partition by(
          column(
            column(c1) auto compress,
            column(c2) no auto compress,
            column(c3),
            row(d1) auto compress,
            row(d2) no auto compress,
            row(d3))
            add 1
            )

        cols is a dictionary whose key is the column name and value True or False
        specifying AUTO COMPRESS or NO AUTO COMPRESS respectively. The columns
        are stored with COLUMN format.

        rows is a dictionary similar to cols except the ROW format is used

        const is an unsigned BIGINT
        """
        res = 'partition by( column all but' if all_but else\
                        'partition by( column'
        c = self._visit_partition_by(cols, rows)
        c += [{'post': (['add %s' % str(const)]
            if const is not None
            else []) + [')']}]

        return self.__class__(self._append(self.opts, {res: c}))

    def _visit_partition_by(self, cols, rows):

        if cols:
            c = ['column('+ k +') auto compress '\
                            for k,v in cols.items() if v is True]

            c += ['column('+ k +') no auto compress'\
                            for k,v in cols.items() if v is False]

            c += ['column('+ k +')' for k,v in cols.items() if v is None]

        if rows:
            c += ['row('+ k +') auto compress'\
                            for k,v in rows.items() if v is True]

            c += ['row('+ k +') no auto compress'\
                            for k,v in rows.items() if v is False]

            c += ['row('+ k +')' for k,v in rows.items() if v is None]

        return c


    def partition_by_col_auto_compress(self, all_but=False, cols={},\
                                       rows={}, const=None):

        res = 'partition by( column auto compress all but' if all_but else\
                        'partition by( column auto compress'
        c = self._visit_partition_by(cols,rows)
        c += [{'post': (['add %s' % str(const)]
            if const is not None
            else []) + [')']}]

        return self.__class__(self._append(self.opts, {res: c}))


    def partition_by_col_no_auto_compress(self, all_but=False, cols={},\
                                          rows={}, const=None):

        res = 'partition by( column no auto compress all but' if all_but else\
                        'partition by( column no auto compression'
        c = self._visit_partition_by(cols,rows)
        c += [{'post': (['add %s' % str(const)]
            if const is not None
            else []) + [')']}]

        return self.__class__(self._append(self.opts, {res: c}))


    def index(self, index):
        """
        Index is created with dialect specific keywords to
        include loading and ordering syntax elements

        index is a sqlalchemy.sql.schema.Index object.
        """
        return self.__class__(self._append(self.opts, {res: c}))


    def unique_index(self, name=None, cols=[]):
        res = 'unique index ' + (name if name is not None else '')
        return self.__class__(self._append(self.opts, {res:cols}))


class TeradataTypeCompiler(compiler.GenericTypeCompiler):

    def _get(self, key, type_, kw):
        return kw.get(key, getattr(type_, key, None))

    def visit_datetime(self, type_, **kw):
        return self.visit_TIMESTAMP(type_, precision=6, **kw)

    def visit_date(self, type_, **kw):
        return self.visit_DATE(type_, **kw)

    def visit_text(self, type_, **kw):
        return self.visit_CLOB(type_, **kw)

    def visit_time(self, type_, **kw):
        return self.visit_TIME(type_, precision=6, **kw)

    def visit_unicode(self, type_, **kw):
        return self.visit_VARCHAR(type_, charset='UNICODE', **kw)

    def visit_unicode_text(self, type_, **kw):
        return self.visit_CLOB(type_, charset='UNICODE', **kw)

    def visit_interval_year(self, type_, **kw):
        return 'INTERVAL YEAR{}'.format(
            '('+str(type_.year_precision)+')' if type_.year_precision else '')

    def visit_interval_year_to_month(self, type_, **kw):
        return 'INTERVAL YEAR{} TO MONTH'.format(
            '('+str(type_.year_precision)+')' if type_.year_precision else '')

    def visit_interval_month(self, type_, **kw):
        return 'INTERVAL MONTH{}'.format(
            '('+str(type_.month_precision)+')' if type_.month_precision else '')

    def visit_interval_day(self, type_, **kw):
        return 'INTERVAL DAY{}'.format(
            '('+str(type_.day_precision)+')' if type_.day_precision else '')

    def visit_interval_day_to_hour(self, type_, **kw):
        return 'INTERVAL DAY{} TO HOUR'.format(
            '('+str(type_.day_precision)+')' if type_.day_precision else '')

    def visit_interval_day_to_minute(self, type_, **kw):
        return 'INTERVAL DAY{} TO MINUTE'.format(
            '('+str(type_.day_precision)+')' if type_.day_precision else '')

    def visit_interval_day_to_second(self, type_, **kw):
        return 'INTERVAL DAY{} TO SECOND{}'.format(
            '('+str(type_.day_precision)+')' if type_.day_precision else '',
            '('+str(type_.frac_precision)+')' if type_.frac_precision is not None  else '')

    def visit_interval_hour(self, type_, **kw):
        return 'INTERVAL HOUR{}'.format(
            '('+str(type_.hour_precision)+')' if type_.hour_precision else '')

    def visit_interval_hour_to_minute(self, type_, **kw):
        return 'INTERVAL HOUR{} TO MINUTE'.format(
            '('+str(type_.hour_precision)+')' if type_.hour_precision else '')

    def visit_interval_hour_to_second(self, type_, **kw):
        return 'INTERVAL HOUR{} TO SECOND{}'.format(
            '('+str(type_.hour_precision)+')' if type_.hour_precision else '',
            '('+str(type_.frac_precision)+')' if type_.frac_precision is not None else '')

    def visit_interval_minute(self, type_, **kw):
        return 'INTERVAL MINUTE{}'.format(
              '('+str(type_.minute_precision)+')' if type_.minute_precision else '')

    def visit_interval_minute_to_second(self, type_, **kw):
        return 'INTERVAL MINUTE{} TO SECOND{}'.format(
            '('+str(type_.minute_precision)+')' if type_.minute_precision else '',
            '('+str(type_.frac_precision)+')' if type_.frac_precision is not None else '')

    def visit_interval_second(self, type_, **kw):
        if type_.frac_precision is not None and type_.second_precision:
          return 'INTERVAL SECOND{}'.format(
              '('+str(type_.second_precision)+', '+str(type_.frac_precision)+')')
        else:
          return 'INTERVAL SECOND{}'.format(
              '('+str(type_.second_precision)+')' if type_.second_precision else '')

    def visit_PERIOD_DATE(self, type_, **kw):
        return 'PERIOD(DATE)' +\
            (" FORMAT '" + type_.format + "'" if type_.format is not None else '')

    def visit_PERIOD_TIME(self, type_, **kw):
        return 'PERIOD(TIME{}{})'.format(
                '(' + str(type_.frac_precision) + ')'
                    if type_.frac_precision is not None
                    else '',
                ' WITH TIME ZONE' if type_.timezone else '') +\
            (" FORMAT '" + type_.format + "'" if type_.format is not None else '')

    def visit_PERIOD_TIMESTAMP(self, type_, **kw):
        return 'PERIOD(TIMESTAMP{}{})'.format(
                '(' + str(type_.frac_precision) + ')'
                    if type_.frac_precision is not None
                    else '',
                ' WITH TIME ZONE' if type_.timezone else '') +\
            (" FORMAT '" + type_.format + "'" if type_.format is not None else '')

    def visit_TIME(self, type_, **kw):
        tz = ' WITH TIME ZONE' if type_.timezone else ''
        prec = self._get('precision', type_, kw)
        prec = '%s' % '('+str(prec)+')' if prec is not None else ''
        return 'TIME{}{}'.format(prec, tz)

    def visit_DATETIME(self, type_, **kw):
        return self.visit_TIMESTAMP(type_, precision=6,  **kw)

    def visit_TIMESTAMP(self, type_, **kw):
        tz = ' WITH TIME ZONE' if type_.timezone else ''
        prec = self._get('precision', type_, kw)
        prec = '%s' % '('+str(prec)+')' if prec is not None else ''
        return 'TIMESTAMP{}{}'.format(prec, tz)

    def _string_process(self, type_, datatype, **kw):
        length = self._get('length', type_, kw)
        length = '(%s)' % length if length is not None  else ''

        charset = self._get('charset', type_, kw)
        charset = ' CHAR SET %s' % charset if charset is not None else ''

        res = '{}{}{}'.format(datatype, length, charset)
        return res

    def visit_NCHAR(self, type_, **kw):
        return self.visit_CHAR(type_, charset='UNICODE', **kw)

    def visit_NVARCHAR(self, type_, **kw):
        return self.visit_VARCHAR(type_, charset='UNICODE', **kw)

    def visit_CHAR(self, type_, **kw):
        return self._string_process(type_, 'CHAR', length=type_.length, **kw)

    def visit_VARCHAR(self, type_, **kw):
        if type_.length is None:
            return self._string_process(type_, 'LONG VARCHAR', **kw)
        else:
            return self._string_process(type_, 'VARCHAR', length=type_.length, **kw)

    def visit_TEXT(self, type_, **kw):
        return self.visit_CLOB(type_, **kw)

    def visit_CLOB(self, type_, **kw):
        multi = self._get('multiplier', type_, kw)
        if multi is not None and type_.length is not None:
            length = str(type_.length) + multi
            return self._string_process(type_, 'CLOB', length=length, **kw)

        return self._string_process(type_, 'CLOB', **kw)

    def visit_BLOB(self, type, **kw):
        pass

    def visit_BOOLEAN(self, type_, **kw):
        return self.visit_BYTEINT(type_, **kw)

    def visit_BYTEINT(self, type_, **kw):
        return 'BYTEINT'



#@compiles(Select, 'teradata')
#def compile_select(element, compiler, **kw):
#    """
#    """
#
#    if not getattr(element, '_window_visit', None):
#      if element._limit is not None or element._offset is not None:
#          limit, offset = element._limit, element._offset
#
#          orderby=compiler.process(element._order_by_clause)
#          if orderby:
#            element = element._generate()
#            element._window_visit=True
#            #element._limit = None
#            #element._offset = None  cant set to none...
#
#            # add a ROW NUMBER() OVER(ORDER BY) column
#            element = element.column(sql.literal_column('ROW NUMBER() OVER (ORDER BY %s)' % orderby).label('rownum')).order_by(None)
#
#            # wrap into a subquery
#            limitselect = sql.select([c for c in element.alias().c if c.key != 'rownum'])
#
#            limitselect._window_visit=True
#            limitselect._is_wrapper=True
#
#            if offset is not None:
#              limitselect.append_whereclause(sql.column('rownum') > offset)
#              if limit is not None:
#                  limitselect.append_whereclause(sql.column('rownum') <= (limit + offset))
#            else:
#              limitselect.append_whereclause(sql.column("rownum") <= limit)
#
#            element = limitselect
#
#    kw['iswrapper'] = getattr(element, '_is_wrapper', False)
#    return compiler.visit_select(element, **kw)
