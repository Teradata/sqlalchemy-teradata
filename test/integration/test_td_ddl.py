from sqlalchemy import Table, Column, MetaData, Index, Integer
from sqlalchemy import testing
from sqlalchemy_teradata import *
from sqlalchemy.testing.plugin.pytestplugin import *
from sqlalchemy_teradata.compiler import TDCreateTablePost, TDCreateTableSuffix

import itertools

"""
Integration testing for DDL Expressions and Dialect Extensions
The tests are based of off SQL Data Definition Language (Release 15.10, Dec '15)
"""

def test_decorator(test_fn):

    def test_wrapper_fn(self):
        test_fn(self)
        self.metadata.create_all(checkfirst=False)
        self._test_tables_created(self.metadata, self.engine)

    test_wrapper_fn.__name__ = test_fn.__name__
    return test_wrapper_fn


class TestCreateIndexDDL(testing.fixtures.TestBase):

    def setup(self):
        self.conn     = testing.db.connect()
        self.engine   = self.conn.engine
        self.metadata = MetaData(bind=self.engine)

    def tearDown(self):
        self.metadata.drop_all(self.engine)
        self.conn.invalidate()
        self.conn.close()

    def _test_tables_created(self, metadata, engine):
        """
        Asserts that all the tables within the passed in metadata exists on the
        database of the specified engine and are the only tables on that
        database.

        Args:
            metadata: A MetaData instance containing the tables to check for.
            engine:   An Engine instance associated with a dialect and database
                      for which the tables in the metadata are to be checked for.

        Raises:
            AssertionError: Raised when the set of tables contained in the
                            metadata is not equal to the set of tables on
                            the engine's associated database.
        """
        assert(
            set([tablename for tablename, _ in metadata.tables.items()]) ==
            set(engine.table_names()))

    # TODO Add a more thorough test to check that indexes are actually being
    #      created on the database
    # def _test_indexes_created(self, metadata, engine):

    @test_decorator
    def test_create_index_inline(self):
        """
        Tests creating tables with an inline column index.
        """
        my_table = Table('t_index_inline', self.metadata,
                        Column('c1', INTEGER),
                        Column('c2', DECIMAL, index=True))

    @test_decorator
    def test_create_index_construct(self):
        """
        Tests creating tables with the schema index construct.
        """
        my_table = Table('t_index_construct', self.metadata,
                        Column('c1', INTEGER),
                        Column('c2', DECIMAL))
        Index('i', my_table.c.c2)

    @test_decorator
    def test_create_multiple_indexes(self):
        """
        Tests creating tables with multiple indexes.
        """
        my_table = Table('t_multiple_indexes', self.metadata,
                        Column('c1', INTEGER),
                        Column('c2', DECIMAL))
        Index('i', my_table.c.c1, my_table.c.c2)

    @test_decorator
    def test_create_index_unique(self):
        """
        Tests creating tables with a unique index.
        """
        my_table = Table('t_index_unique', self.metadata,
                        Column('c', INTEGER, index=True, unique=True))

    @test_decorator
    def test_create_multiple_unique_indexes(self):
        """
        Tests creating tables with multiple unique indexes.
        """
        my_table = Table('t_multiple_unique_indexes', self.metadata,
                        Column('c1', INTEGER),
                        Column('c2', DECIMAL))
        Index('i', my_table.c.c1, my_table.c.c2, unique=True)

    @test_decorator
    def test_create_index_noname(self):
        """
        Tests creating tables with an index without an index name.
        """
        my_table = Table('t_index_noname', self.metadata,
                        Column('c1', INTEGER),
                        Column('c2', DECIMAL))
        Index(None, my_table.c.c2)


class TestCreateSuffixDDL(testing.fixtures.TestBase):

    def setup(self):
        self.conn     = testing.db.connect()
        self.engine   = self.conn.engine
        self.metadata = MetaData(bind=self.engine)

    def tearDown(self):
        self.metadata.drop_all(self.engine)
        self.conn.invalidate()
        self.conn.close()

    def _generate_table_name(self, base, opt):
        """
        Generates a unique table name for each base and option argument
        with the following form:

            't_base_opt1_op2 ... _optn'

        Args:
            base: The base name of the table.
            opt:  The option passed to the table generation process. This is
                  expected to either be a primitive or a tuple of primitives.

        Returns:
            A unique table name of a particular form based on the
            specified arguments.
        """
        return 't_' + base + '_' +\
            ('_'.join([str(arg).lower() for
                arg in opt if arg is not None])
            if isinstance(opt, tuple)
            else str(opt).lower())

    def _create_tables_with_suffix_opts(self, suffix, opts, metadata):
        """
        Create tables each with a particular TDCreateTableSuffix over various
        options of the suffix (and bind each table to the passed in metadata).

        Args:
            suffix:   The TDCreateTableSuffix (function) to create the
                      tables with.
            opts:     The various options of the suffix to create the
                      tables with. This is expected to either be a list of
                      tuples or list of primitives.
            metadata: The metadata to bind all created tables to.
        """
        for opt in opts:
            Table(
                self._generate_table_name(suffix.__name__, opt),
                metadata,
                Column('c', Integer),
                teradata_suffixes=suffix(*opt)
                    if isinstance(opt, tuple)
                    else suffix(opt))

    def _test_tables_created(self, metadata, engine):
        """
        Asserts that all the tables within the passed in metadata exists on the
        database of the specified engine and are the only tables on that
        database.

        Args:
            metadata: A MetaData instance containing the tables to check for.
            engine:   An Engine instance associated with a dialect and database
                      for which the tables in the metadata are to be checked for.

        Raises:
            AssertionError: Raised when the set of tables contained in the
                            metadata is not equal to the set of tables on
                            the engine's associated database.
        """
        assert(
            set([tablename for tablename, _ in metadata.tables.items()]) ==
            set(engine.table_names()))

    @test_decorator
    def test_create_suffix_fallback(self):
        """
        Tests creating tables with the fallback suffix and the following
        option(s):

            enabled = True, False
        """
        opts = (True, False)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().fallback, opts, self.metadata)

    @test_decorator
    def test_create_suffix_log(self):
        """
        Tests creating tables with the log suffix and the following
        option(s):

            enabled = True
        """
        opts = (True,)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().log, opts, self.metadata)

    def test_create_suffix_nolog_err(self):
        """
        Tests for specific error(s) when creating tables with the log suffix
        and the following option(s):

            enabled = False
        """
        with pytest.raises(Exception) as exc_info:
            opts = (False,)
            self._create_tables_with_suffix_opts(
                TDCreateTableSuffix().log, opts, self.metadata)

            self.metadata.create_all(checkfirst=False)

        assert('NO LOG keywords not allowed for permanent table' in
            str(exc_info.value))
        assert(not self.engine.has_table('t_log_false'))

    # TODO Add test(s) for journaling (currently unable to create a new permanent
    #      journal for the database through MODIFY, can through CREATE but
    #      creating a new database during testing is not ideal)
    # def test_create_suffix_journal(self):

    @test_decorator
    def test_create_suffix_checksum(self):
        """
        Tests creating tables with the checksum suffix and the following
        option(s):

            integrity_checking = 'default', 'off', 'on'
        """
        opts = ('default', 'off', 'on')
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().checksum, opts, self.metadata)

    @test_decorator
    def test_create_suffix_freespace(self):
        """
        Tests creating tables with the freespace suffix and the following
        option(s):

            percentage = 0, 75, 40
        """
        opts = (0, 75, 40)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().freespace, opts, self.metadata)

    def test_create_suffix_freespace_err(self):
        """
        Tests for specific error(s) when creating tables with the freespace suffix
        and the following option(s):

            percentage = 100
        """
        with pytest.raises(Exception) as exc_info:
            opts = (100,)
            self._create_tables_with_suffix_opts(
                TDCreateTableSuffix().freespace, opts, self.metadata)

            self.metadata.create_all(checkfirst=False)

        assert('The specified FREESPACE value is not between 0 and 75 percent' in
            str(exc_info.value))
        assert(not self.engine.has_table('t_freespace_100'))

    @test_decorator
    def test_create_suffix_mergeblockratio(self):
        """
        Tests creating tables with the mergeblockratio suffix and the following
        option(s):

            integer = None, 0, 100, 50
        """
        opts = (None, 0, 100, 50)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().mergeblockratio, opts, self.metadata)

        Table('t_no_mergeblockratio', self.metadata,
            Column('c', Integer),
            teradata_suffixes=TDCreateTableSuffix().no_mergeblockratio())

    def test_create_suffix_mergeblockratio_err(self):
        """
        Tests for specific error(s) when creating tables with the mergeblockratio
        suffix and the following option(s):

            integer = 101
        """
        with pytest.raises(Exception) as exc_info:
            opts = (101,)
            self._create_tables_with_suffix_opts(
                TDCreateTableSuffix().mergeblockratio, opts, self.metadata)

            self.metadata.create_all(checkfirst=False)

        assert('The specified MERGEBLOCKRATIO value is invalid' in
            str(exc_info.value))
        assert(not self.engine.has_table('t_mergeblockratio_101'))

    @test_decorator
    def test_create_suffix_datablocksize(self):
        """
        Tests creating tables with the datablocksize suffix and the following
        option(s):

            (datablocksize) data_block_size = None, 21248
            min_datablocksize
            max_datablocksize
        """
        opts = (None, 21248)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().datablocksize, opts, self.metadata)

        Table('t_datablocksize_min', self.metadata,
            Column('c', Integer),
            teradata_suffixes=TDCreateTableSuffix().min_datablocksize())

        Table('t_datablocksize_max', self.metadata,
            Column('c', Integer),
            teradata_suffixes=TDCreateTableSuffix().max_datablocksize())

    def test_create_suffix_datablocksize_err(self):
        """
        Tests for specific error(s) when creating tables with the datablocksize
        suffix and the following option(s):

            data_block_size = 1024
        """
        with pytest.raises(Exception) as exc_info:
            opts = (1024,)
            self._create_tables_with_suffix_opts(
                TDCreateTableSuffix().datablocksize, opts, self.metadata)

            self.metadata.create_all(checkfirst=False)

        assert('The specified DATABLOCKSIZE value must be within ' \
            'the range of 21248 and 1048319 bytes' in
            str(exc_info.value))
        assert(not self.engine.has_table('t_datablocksize_1024'))

    @test_decorator
    def test_create_suffix_blockcompression(self):
        """
        Tests creating tables with the blockcompression suffix and the following
        option(s):

            opt = 'default', 'autotemp', 'manual', 'never'
        """
        opts = ('default', 'autotemp', 'manual', 'never')
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().blockcompression, opts, self.metadata)

    @test_decorator
    def test_create_suffix_no_isolated_loading(self):
        """
        Tests creating tables with the no_isolated_loading suffix and the following
        option(s):

            concurrent = False, True
        """
        opts = (False, True)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().with_no_isolated_loading, opts, self.metadata)

    @test_decorator
    def test_create_suffix_isolated_loading(self):
        """
        Tests creating tables with the isolated_loading suffix and the following
        (combination(s) of) option(s):

            concurrent = False, True
            opts       = 'all', 'insert', 'none', None
        """
        concurrent_opts = (True, False)
        for_opts        = ('all', 'insert', 'none', None)
        self._create_tables_with_suffix_opts(
            TDCreateTableSuffix().with_isolated_loading,
            itertools.product(concurrent_opts, for_opts), self.metadata)


class TestCreatePostCreateDDL(testing.fixtures.TestBase):

    def setup(self):
        self.conn     = testing.db.connect()
        self.engine   = self.conn.engine
        self.metadata = MetaData(bind=self.engine)

    def tearDown(self):
        self.metadata.drop_all(self.engine)
        self.conn.invalidate()
        self.conn.close()

    def _generate_table_name(self, base, opt):
        """
        Generates a unique table name for each base and option argument
        with the following form:

            't_base_opt1_op2 ... _optn'

        Args:
            base: The base name of the table.
            opt:  The option passed to the table generation process. This is
                  expected to either be a primitive or a tuple of either
                  primitives, tuples, lists, or dicts.

        Returns:
            A unique table name of a particular form based on the
            specified arguments.
        """
        return 't_' + base + '_' +\
            ('_'.join(
                [('_'.join([str(e).lower() for e in arg])
                if isinstance(arg, (tuple, list, dict))
                else str(arg).lower()) for
                    arg in opt if arg is not None])
            if isinstance(opt, tuple)
            else str(opt).lower())

    def _create_tables_with_post_opts(self, post, opts, metadata):
        """
        Create tables each with a particular TDCreateTablePost over various
        options of the post_create (and bind each table to the passed in metadata).

        Args:
            post:     The TDCreateTablePost (function) to create the
                      tables with.
            opts:     The various options of the post_create to create the
                      tables with. This is expected to either be a list of
                      tuples or list of primitives.
            metadata: The metadata to bind all created tables to.
        """
        for opt in opts:
            Table(
                self._generate_table_name(post.__name__, opt),
                metadata,
                Column('c1', INTEGER),
                Column('c2', DECIMAL),
                Column('c3', VARCHAR),
                Column('c4', CHAR),
                Column('c5', CLOB),
                teradata_post_create=post(*opt)
                    if isinstance(opt, tuple)
                    else post(opt))

    def _test_tables_created(self, metadata, engine):
        """
        Asserts that all the tables within the passed in metadata exists on the
        database of the specified engine and are the only tables on that
        database.

        Args:
            metadata: A MetaData instance containing the tables to check for.
            engine:   An Engine instance associated with a dialect and database
                      for which the tables in the metadata are to be checked for.

        Raises:
            AssertionError: Raised when the set of tables contained in the
                            metadata is not equal to the set of tables on
                            the engine's associated database.
        """
        assert(
            set([tablename for tablename, _ in metadata.tables.items()]) ==
            set(engine.table_names()))

    @test_decorator
    def test_create_post_no_primary_index(self):
        """
        Tests creating tables with the no_primary_index post_create.
        """
        Table('t_no_primary_index', self.metadata,
            Column('c', Integer),
            teradata_post_create=TDCreateTablePost().no_primary_index())

    @test_decorator
    def test_create_post_primary_index(self):
        """
        Tests creating tables with the primary_index post_create and the following
        (combination(s) of) option(s):

            name   = None, 'primary_index_name'
            unique = False, True
            cols   = ['c1'], ['c1', 'c3']
        """
        name_opts   = (None, 'primary_index_name')
        unique_opts = (False, True)
        cols_opts   = (['c1'], ['c1', 'c3'])
        self._create_tables_with_post_opts(
            TDCreateTablePost().primary_index,
            itertools.product(name_opts, unique_opts, cols_opts), self.metadata)

    # TODO Add test(s) for primary_amp (which requires partition_by)
    # def test_create_post_primary_amp(self):
    #     name_opts = (None, "primary_amp_name")
    #     cols_opts = (['c1'], ['c1', 'c3'])
    #     self._create_tables_with_post_opts(
    #         TDCreateTablePost().primary_amp,
    #         itertools.product(name_opts, cols_opts), self.metadata)
    #
    #     self.metadata.create_all(checkfirst=False)
    #     self._test_tables_created(self.metadata, self.engine)

    # TODO Add test(s) for partition_by_col (which is currently not working due
    #      to issues with "column partitioning not supported by system")
    # def test_create_post_partition_by(self):
    #     all_but_opts = (False, True)
    #     cols_opts    = (
    #         {'c1': True},
    #         {'c1': True, 'c3': False, 'c5': None})
    #     rows_opts    = (
    #         {'d1': True},
    #         {'d1': True, 'd3': False, 'd5': None})
    #     const_opts   = (None, 1)
    #
    #     self._create_tables_with_post_opts(
    #         TDCreateTablePost().no_primary_index().partition_by_col,
    #         itertools.product(all_but_opts, cols_opts, rows_opts, const_opts), self.metadata)
    #
    #     self.metadata.create_all(checkfirst=False)
    #     self._test_tables_created(self.metadata, self.engine)

    @test_decorator
    def test_create_post_unique_index(self):
        """
        Tests creating tables with the unique_index post_create and the following
        (combination(s) of) option(s):

            name = None, 'unique_index_name'
            cols = ['c2'], ['c2', 'c3']
        """
        name_opts = (None, 'unique_index_name')
        cols_opts = (['c2'], ['c2', 'c3'])
        self._create_tables_with_post_opts(
            TDCreateTablePost().unique_index,
            itertools.product(name_opts, cols_opts), self.metadata)

    def test_create_post_unique_index_err(self):
        """
        Tests for specific error(s) when creating tables with the unique_index
        post_create and the following (combination(s) of) option(s):

            name = None
            cols = ['c1']
        """
        with pytest.raises(Exception) as exc_info:
            opts = [(None, ['c1'])]
            self._create_tables_with_post_opts(
                TDCreateTablePost().unique_index, opts, self.metadata)

            self.metadata.create_all(checkfirst=False)

        assert('Two indexes with the same columns' in str(exc_info.value))
        assert(not self.engine.has_table('t_unique_index_c1'))
