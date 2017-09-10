# sqlalchemy_teradata/dialect.py
# Copyright (C) 2015-2016 by Teradata
# <see AUTHORS file>
#
# This module is part of sqlalchemy-teradata and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from sqlalchemy.engine import default
from sqlalchemy import pool, String, Numeric
from sqlalchemy.sql import select, and_, or_
from sqlalchemy_teradata.compiler import TeradataCompiler, TeradataDDLCompiler, TeradataTypeCompiler
from sqlalchemy_teradata.base import TeradataIdentifierPreparer, TeradataExecutionContext
from sqlalchemy.sql.expression import text, table, column, asc
from sqlalchemy import Table, Column, Index
import sqlalchemy.types as sqltypes
import sqlalchemy_teradata.types as tdtypes
from itertools import groupby
from teradata.tdodbc import osType

# ischema names is used for reflecting columns (see get_columns in the dialect)
ischema_names = {
    None: sqltypes.NullType,
    
    'cf': tdtypes.CHAR,
    'cv': tdtypes.VARCHAR,
    'uf': sqltypes.NCHAR,
    'uv': sqltypes.NVARCHAR,
    'co': tdtypes.CLOB,
    'n' : tdtypes.NUMERIC,
    'd' : tdtypes.DECIMAL,
    'i' : sqltypes.INTEGER,
    'i1': tdtypes.BYTEINT,
    'i2': sqltypes.SMALLINT,
    'i8': sqltypes.BIGINT,
    'f' : sqltypes.FLOAT,
    'da': sqltypes.DATE,
    'ts': tdtypes.TIMESTAMP,
    'sz': tdtypes.TIMESTAMP,    #Added timestamp with timezone
    'at': tdtypes.TIME,
    'tz': tdtypes.TIMESTAMP,    #Added time with timezone
    
    #Expreimental - Binary
    'bf': sqltypes.BINARY,
    'bv': sqltypes.VARBINARY,
    'bo': sqltypes.BLOB
} #TODO: add the interval types and blob

stringtypes=[ t for t in ischema_names if issubclass(ischema_names[t],sqltypes.String)]
        
class TeradataDialect(default.DefaultDialect):

    name = 'teradata'
    driver = 'teradata'
    default_paramstyle = 'qmark'
    
    #Connection pooling not supported by Linux ODBC driver
    if osType=='Linux':
        poolclass = pool.NullPool
    else:
        poolclass = pool.SingletonThreadPool

    statement_compiler = TeradataCompiler
    ddl_compiler = TeradataDDLCompiler
    type_compiler = TeradataTypeCompiler
    preparer = TeradataIdentifierPreparer
    execution_ctx_cls = TeradataExecutionContext

    supports_native_boolean = False
    supports_native_decimal = True
    supports_unicode_statements = True
    supports_unicode_binds = True
    postfetch_lastrowid = False
    implicit_returning = False
    preexecute_autoincrement_sequences = False

    construct_arguments = [
      (Table, {
              "post_create": None,
              "postfixes": None
       }),

      (Index, {
          "order_by": None,
          "loading": None
       }),

      (Column, {
          "compress": None,
          "identity": None
      })
    ]

    def __init__(self, **kwargs):
        super(TeradataDialect, self).__init__(**kwargs)

    def create_connect_args(self, url):
      if url is not None:
        params = super(TeradataDialect, self).create_connect_args(url)[1]
        cargs = ("Teradata", params['host'], params['username'], params['password'])
        cparams = {p:params[p] for p in params if p not in\
                                ['host', 'username', 'password']}
        return (cargs, cparams)

    @classmethod
    def dbapi(cls):

        """ Hook to the dbapi2.0 implementation's module"""
        from teradata import tdodbc
        return tdodbc

    def normalize_name(self, name, **kw):
        if name is not None:
            return name.strip().lower()
        return name

    def has_table(self, connection, table_name, schema=None):

        if schema is None:
            schema=self.default_schema_name

        stmt = select([column('tablename')],
                      from_obj=[text('dbc.tablesvx')]).where(
                        and_(text('DatabaseName=:schema'),
                             text('TableName=:table_name')))

        res = connection.execute(stmt, schema=schema, table_name=table_name).fetchone()
        return res is not None

    def _resolve_type(self, t, **kw):
        """
        Resolve types for String, Numeric, Date/Time, etc. columns
        """
        t = self.normalize_name(t)
        if t in ischema_names:
            #print(t,ischema_names[t])
            t = ischema_names[t]
            
            if issubclass(t, sqltypes.String):
                return t(length=kw['length']/2 if kw['chartype']=='UNICODE' else kw['length'],\
                            charset=kw['chartype'])

            elif issubclass(t, sqltypes.Numeric):
                return t(precision=kw['prec'], scale=kw['scale'])

            elif issubclass(t, sqltypes.Time) or issubclass(t, sqltypes.DateTime):
                #Timezone
                tz=kw['fmt'][-1]=='Z'

                #Precision                
                prec = kw['fmt']    
                #For some timestamps and dates, there is no precision, or indicatd in scale
                prec = prec[prec.index('(') + 1: prec.index(')')] if '(' in prec else 0
                prec = kw['scale'] if prec=='F' else int(prec)

                #prec = int(prec[prec.index('(') + 1: prec.index(')')]) if '(' in prec else 0
                return t(precision=prec,timezone=tz)

            elif issubclass(t, sqltypes.Interval):
                return t(day_precision=kw['prec'],second_precision=kw['scale'])

            else:
                return t() # For types like Integer, ByteInt

        return ischema_names[None]

    def _get_column_info(self, row):
        """
        Resolves the column information for get_columns given a row.
        """
        chartype = {
                  0: None,
                  1: 'LATIN',
                  2: 'UNICODE',
                  3: 'KANJISJIS',
                  4: 'GRAPHIC'}
        
        #Handle unspecified characterset and disregard chartypes specified for non-character types (e.g. binary, json)
        typ = self._resolve_type(row['columntype'],\
                                    length=int(row['columnlength'] or 0),\
                                    chartype=chartype[row['chartype'] if row['chartype'] in stringtypes else 0],\
                                    prec=int(row['decimaltotaldigits'] or 0),\
                                    scale=int(row['decimalfractionaldigits'] or 0),\
                                    fmt=row['columnformat'])

        autoinc = row['idcoltype'] in ('GA', 'GD')

        return {
                'name': self.normalize_name(row['columnname']),
                'type': typ,
                'nullable': row['nullable'] == u'Y',
                'default': row['defaultvalue'],
                'attrs': {
                    'columnformat':row['columnformat']},
                'autoincrement': autoinc
               }


    def get_columns(self, connection, table_name, schema=None, **kw):

        helpView=False
        
        if schema is None:
            schema = self.default_schema_name
        
        if int(self.server_version_info.split('.')[0])<16:
            dbc_columninfo='dbc.ColumnsV'

            #Check if the object us a view
            stmt = select([column('tablekind')],\
                            from_obj=[text('dbc.tablesV')]).where(\
                            and_(text('DatabaseName=:schema'),\
                                 text('TableName=:table_name'),\
                                 text("tablekind='V'")))
            res = connection.execute(stmt, schema=schema, table_name=table_name).rowcount
            helpView = (res==1)

        else:
            dbc_columninfo='dbc.ColumnsQV'
        
        stmt = select([column('columnname'), column('columntype'),\
                        column('columnlength'), column('chartype'),\
                        column('decimaltotaldigits'), column('decimalfractionaldigits'),\
                        column('columnformat'),\
                        column('nullable'), column('defaultvalue'), column('idcoltype')],\
                        from_obj=[text(dbc_columninfo)]).where(\
                        and_(text('DatabaseName=:schema'),\
                             text('TableName=:table_name')))

        res = connection.execute(stmt, schema=schema, table_name=table_name).fetchall()
        
        #If this is a view in pre-16 version, get types for individual columns
        if helpView:
            res=[self._get_column_help(connection, schema,table_name,r['columnname']) for r in res]
            
        return [self._get_column_info(row) for row in res]

    def _get_default_schema_name(self, connection):
        return self.normalize_name(
            connection.execute('select database').scalar())

    def _get_column_help(self, connection, schema,table_name,column_name):
        stmt='help column '+schema+'.'+table_name+'.'+column_name
        res = connection.execute(stmt).fetchall()[0]
        
        return {'columnname':res['Column Name'],
                'columntype':res['Type'],
                'columnlength':res['Max Length'],
                'chartype':res['Char Type'],
                'decimaltotaldigits':res['Decimal Total Digits'],
                'decimalfractionaldigits':res['Decimal Fractional Digits'],
                'columnformat':res['Format'],
                'nullable':res['Nullable'],
                'defaultvalue':None,
                'idcoltype':res['IdCol Type']
                }
    
    def get_table_names(self, connection, schema=None, **kw):

        if schema is None:
            schema = self.default_schema_name

        stmt = select([column('tablename')],
                      from_obj=[text('dbc.TablesVX')]).where(
                      and_(text('DatabaseName = :schema'),
                          or_(text('tablekind=\'T\''),
                              text('tablekind=\'O\''))))
        res = connection.execute(stmt, schema=schema).fetchall()
        return [self.normalize_name(name['tablename']) for name in res]

    def get_schema_names(self, connection, **kw):
        stmt = select([column('username')],
               from_obj=[text('dbc.UsersV')],
               order_by=[text('username')])
        res = connection.execute(stmt).fetchall()
        return [self.normalize_name(name['username']) for name in res]

    def get_view_definition(self, connection, view_name, schema=None, **kw):

        if schema is None:
             schema = self.default_schema_name

        res = connection.execute('show table {}.{}'.format(schema, view_name)).scalar()
        return self.normalize_name(res)

    def get_view_names(self, connection, schema=None, **kw):

        if schema is None:
            schema = self.default_schema_name

        stmt = select([column('tablename')],
                      from_obj=[text('dbc.TablesVX')]).where(
                      and_(text('DatabaseName = :schema'),
                           text('tablekind=\'V\'')))

        res = connection.execute(stmt, schema=schema).fetchall()
        return [self.normalize_name(name['tablename']) for name in res]

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """
        Override
        TODO: Check if we need PRIMARY Indices or PRIMARY KEY Indices
        TODO: Check for border cases (No PK Indices)
        """

        if schema is None:
            schema = self.default_schema_name

        stmt = select([column('ColumnName'), column('IndexName')],
                      from_obj=[text('dbc.Indices')]).where(
                          and_(text('DatabaseName = :schema'),
                              text('TableName=:table'),
                              text('IndexType=:indextype'))
                      ).order_by(asc(column('IndexNumber')))

        # K for Primary Key
        res = connection.execute(stmt, schema=schema, table=table_name, indextype='K').fetchall()

        index_columns = list()
        index_name = None

        for index_column in res:
            index_columns.append(self.normalize_name(index_column['ColumnName']))
            index_name = self.normalize_name(index_column['IndexName']) # There should be just one IndexName

        return {
            "constrained_columns": index_columns,
            "name": index_name
        }

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        """
        Overrides base class method
        """
        if schema is None:
            schema = self.default_schema_name

        stmt = select([column('ColumnName'), column('IndexName')], from_obj=[text('dbc.Indices')]) \
            .where(and_(text('DatabaseName = :schema'),
                        text('TableName=:table'),
                        text('IndexType=:indextype'))) \
            .order_by(asc(column('IndexName')))

        # U for Unique
        res = connection.execute(stmt, schema=schema, table=table_name, indextype='U').fetchall()

        def grouper(fk_row):
            return {
                'name': self.normalize_name(fk_row['IndexName']),
            }

        unique_constraints = list()
        for constraint_info, constraint_cols in groupby(res, grouper):
            unique_constraint = {
                'name': self.normalize_name(constraint_info['name']),
                'column_names': list()
            }

            for constraint_col in constraint_cols:
                unique_constraint['column_names'].append(self.normalize_name(constraint_col['ColumnName']))

            unique_constraints.append(unique_constraint)

        return unique_constraints

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """
        Overrides base class method
        """

        if schema is None:
            schema = self.default_schema_name

        stmt = select([column('IndexID'), column('IndexName'), column('ChildKeyColumn'), column('ParentDB'),
                       column('ParentTable'), column('ParentKeyColumn')],
                      from_obj=[text('DBC.All_RI_ChildrenV')]) \
            .where(and_(text('ChildTable = :table'),
                        text('ChildDB = :schema'))) \
            .order_by(asc(column('IndexID')))

        res = connection.execute(stmt, schema=schema, table=table_name).fetchall()

        def grouper(fk_row):
            return {
                'name': fk_row.IndexName or fk_row.IndexID, #ID if IndexName is None
                'schema': fk_row.ParentDB,
                'table': fk_row.ParentTable
            }

        # TODO: Check if there's a better way
        fk_dicts = list()
        for constraint_info, constraint_cols in groupby(res, grouper):
            fk_dict = {
                'name': constraint_info['name'],
                'constrained_columns': list(),
                'referred_table': constraint_info['table'],
                'referred_schema': constraint_info['schema'],
                'referred_columns': list()
            }

            for constraint_col in constraint_cols:
                fk_dict['constrained_columns'].append(self.normalize_name(constraint_col['ChildKeyColumn']))
                fk_dict['referred_columns'].append(self.normalize_name(constraint_col['ParentKeyColumn']))

            fk_dicts.append(fk_dict)

        return fk_dicts

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """
        Overrides base class method
        """

        if schema is None:
            schema = self.default_schema_name

        stmt = select(["*"], from_obj=[text('dbc.Indices')]) \
            .where(and_(text('DatabaseName = :schema'),
                        text('TableName=:table'))) \
            .order_by(asc(column('IndexName')))

        res = connection.execute(stmt, schema=schema, table=table_name).fetchall()

        def grouper(fk_row):
            return {
                'name': fk_row.IndexName or fk_row.IndexNumber, # If IndexName is None TODO: Check what to do
                'unique': True if fk_row.UniqueFlag == 'Y' else False
            }

        # TODO: Check if there's a better way
        indices = list()
        for index_info, index_cols in groupby(res, grouper):
            index_dict = {
                'name': index_info['name'],
                'column_names': list(),
                'unique': index_info['unique']
            }

            for index_col in index_cols:
                index_dict['column_names'].append(self.normalize_name(index_col['ColumnName']))

            indices.append(index_dict)

        return indices

    def get_transaction_mode(self, connection, **kw):
        """
        Returns the transaction mode set for the current session.
        T = TDBS
        A = ANSI
        """
        stmt = select([text('transaction_mode')],\
                from_obj=[text('dbc.sessioninfov')]).\
                where(text('sessionno=SESSION'))

        res = connection.execute(stmt).scalar()
        return res

    def _get_server_version_info(self, connection, **kw):
        """
        Returns the Teradata Database software version.
        """
        stmt = select([text('InfoData')],\
                from_obj=[text('dbc.dbcinfov')]).\
                where(text('InfoKey=\'VERSION\''))

        res = connection.execute(stmt).scalar()
        return res

    def conn_supports_autocommit(self, connection, **kw):
        """
        Returns True if autocommit is used for this connection (underlying Teradata session)
        else False
        """
        return self.get_transaction_mode(connection) == 'T'

dialect = TeradataDialect
