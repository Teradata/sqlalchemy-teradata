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


class TeradataCompiler(compiler.SQLCompiler):

    def __init__(self, dialect, statement, column_keys=None, inline=False, **kwargs):
        super(TeradataCompiler, self).__init__(dialect, statement, column_keys, inline, **kwargs)

    def get_select_precolumns(self, select, **kwargs):
        """Teradata uses TOP instead of LIMIT """

        if select._distinct or select._limit is not None:
            s = select._distinct and "DISTINCT " or ""
            if select._limit is not None:
                s += "TOP %d " % (select._limit)
            if select._offset is not None:
                raise exc.InvalidRequestError('Teradata does not support LIMIT with an offset')
            return s
        return compiler.SQLCompiler.get_select_precolumns(self, select)

    def limit_clause(self, select):
        """Limit after SELECT"""
        return ""

class TeradataDDLCompiler(compiler.DDLCompiler):

    def postfix(self, table):

        """
        This hook processes the optional keyword teradata_postfixes
        ex.
        from sqlalchemy_teradata.compiler import\
                        TDCreateTablePostfix as Opts
        t = Table( 'name', meta,
                   ...,
                   teradata_postfixes=Opts.
                                      fallback().
                                      log().
                                      with_journal_table(t2.name)

        CREATE TABLE name, fallback,
        log,
        with journal table = [database/user.]table_name(
          ...
        )

        teradata_postfixes can also be a list of strings to be appended
        in the order given.
        """
        post=table.dialect_kwargs['teradata_postfixes']

        if isinstance(post, TDCreateTablePostfix):
            if post.opts:
                return ',\n' + post.compile()
            else:
                return post
        elif post:
            assert type(post) is list
            res = ',\n ' + ',\n'.join(post)
        else:
            return ''

    def visit_create_table(self, create):

        """
        Current workaround for https://gerrit.sqlalchemy.org/#/c/85/1
        Once the merge gets released, delete this method entirely
        """
        table = create.element
        preparer = self.dialect.identifier_preparer

        text = '\nCREATE '
        if table._prefixes:
            text += ' '.join(table._prefixes) + ' '
        text += 'TABLE ' + preparer.format_table(table) + ' ' +\
                        self.postfix(table) + ' ('

        separator = '\n'
        # if only one primary key, specify it along with the column
        first_pk = False
        for create_column in create.columns:
            column = create_column.element
            try:
                processed = self.process(create_column,
                                         first_pk=column.primary_key
                                         and not first_pk)
                if processed is not None:
                    text += separator
                    separator = ', \n'
                    text += '\t' + processed
                if column.primary_key:
                    first_pk = True
            except exc.CompileError as ce:
                util.raise_from_cause(
                    exc.CompileError(
                        util.u("(in table '%s', column '%s'): %s") %
                        (table.description, column.name, ce.args[0])
                    ))

        const = self.create_table_constraints(
            table, _include_foreign_key_constraints=  # noga
                create.include_foreign_key_constraints)
        if const:
            text += ', \n\t' + const

        text += "\n)%s\n\n" % self.post_create_table(table)
        return text


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

class TDCreateTablePostfix(TeradataOptions):
    """
    A generative class for Teradata create table options
    specified in teradata_postfixes
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
        res = prefix+' '+'before journal'
        return self.__class__(self._append(self.opts, {res:None}))

    def after_journal(self, prefix='not local'):
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

    def with_no_isolated_loading(self):
        return self.__class__(self._append(self.opts,\
                        {'with no isolated loading':None}))

    def with_concurrent_isolated_loading(self, opt=None):
        """
        opt is a string that takes values 'all', 'insert', or 'none'
        """
        assert opt in ('all', 'insert', 'none')
        for_stmt = ' for '+opt if opt is not None else ''
        res = 'with concurrent isolated loading'+for_stmt
        return self.__class__(self._append(self.opts, {res:opt}))

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
        if const is not None:
            c += [{'post': ['add %s' % str(const), ')']}]

        return self.__class__(self._append(self.opts, {res: c}))

    def _visit_partition_by(self, cols, rows):

        if cols:
            c = ['column('+ k +') auto compress '\
                            for k,v in cols.items() if v is True]

            c += ['column('+ k +') no auto compress'\
                            for k,v in cols.items() if v is False]

            c += ['column(k)' for k,v in cols.items() if v is None]

        if rows:
            c += ['row('+ k +') auto compress'\
                            for k,v in rows.items() if v is True]

            c += ['row('+ k +') no auto compress'\
                            for k,v in rows.items() if v is False]

            c += [k for k,v in rows.items() if v is None]

        return c


    def partition_by_col_auto_compress(self, all_but=False, cols={},\
                                       rows={}, const=None):

        res = 'partition by( column auto compress all but' if all_but else\
                        'partition by( column auto compress'
        c = self._visit_partition_by(cols,rows)
        if const is not None:
              c += [{'post': ['add %s' % str(const), ')']}]

        return self.__class__(self._append(self.opts, {res: c}))


    def partition_by_col_no_auto_compress(self, all_but=False, cols={},\
                                          rows={}, const=None):

        res = 'partition by( column no auto compress all but' if all_but else\
                        'partition by( column no auto compression'
        c = self._visit_partition_by(cols,rows)
        if const is not None:
              c += [{'post': ['add %s' % str(const), ')']}]

        return self.__class__(self._append(self.opts, {res: c}))


    def index(self, index):
        """
        Index is created with dialect specific keywords to
        include loading and ordering syntax elements

        index is a sqlalchemy.sql.schema.Index object.
        """
        return self.__class__(self._append(self.opts, {res: c}))


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
             '('+str(type_.precision)+')' if type_.precision else '')

   def visit_interval_year_to_month(self, type_, **kw):
       return 'INTERVAL YEAR{} TO MONTH'.format(
             '('+str(type_.precision)+')' if type_.precision else '')

   def visit_interval_month(self, type_, **kw):
       return 'INTERVAL MONTH{}'.format(
             '('+str(type_.precision)+')' if type_.precision else '')

   def visit_interval_day(self, type_, **kw):
       return 'INTERVAL DAY{}'.format(
             '('+str(type_.precision)+')' if type_.precision else '')

   def visit_interval_day_to_hour(self, type_, **kw):
       return 'INTERVAL DAY{} TO HOUR'.format(
             '('+str(type_.precision)+')' if type_.precision else '')

   def visit_interval_day_to_minute(self, type_, **kw):
       return 'INTERVAL DAY{} TO MINUTE'.format(
             '('+str(type_.precision)+')' if type_.precision else '')

   def visit_interval_day_to_second(self, type_, **kw):
       return 'INTERVAL DAY{} TO SECOND{}'.format(
             '('+str(type_.precision)+')' if type_.precision else '',
             '('+str(type_.frac_precision)+')' if type_.frac_precision is not None  else ''
             )

   def visit_interval_hour(self, type_, **kw):
       return 'INTERVAL HOUR{}'.format(
             '('+str(type_.precision)+')' if type_.precision else '')

   def visit_interval_hour_to_minute(self, type_, **kw):
       return 'INTERVAL HOUR{} TO MINUTE'.format(
             '('+str(type_.precision)+')' if type_.precision else '')


   def visit_interval_hour_to_second(self, type_, **kw):
       return 'INTERVAL HOUR{} TO SECOND{}'.format(
             '('+str(type_.precision)+')' if type_.precision else '',
             '('+str(type_.frac_precision)+')' if type_.frac_precision is not None else ''
             )

   def visit_interval_minute(self, type_, **kw):
       return 'INTERVAL MINUTE{}'.format(
             '('+str(type_.precision)+')' if type_.precision else '')

   def visit_interval_minute_to_second(self, type_, **kw):
       return 'INTERVAL MINUTE{} TO SECOND{}'.format(
             '('+str(type_.precision)+')' if type_.precision else '',
             '('+str(type_.frac_precision)+')' if type_.frac_precision is not None else ''
             )

   def visit_interval_second(self, type_, **kw):
       if type_.frac_precision is not None and type_.precision:
         return 'INTERVAL SECOND{}'.format(
             '('+str(type_.precision)+', '+str(type_.frac_precision)+')')
       else:
         return 'INTERVAL SECOND{}'.format(
             '('+str(type_.precision)+')' if type_.precision else '')

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
       return 'BYTEINT'
