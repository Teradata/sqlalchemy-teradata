# Change Log

## [v0.9.0](https://github.com/Teradata/sqlalchemy-teradata/tree/HEAD)

The central theme of development for this release is no doubt the dialect’s type system. A large portion of work was dedicated to reworking the types to be more up to date with SQLAlchemy standards and adding useful features to our types as we see fit. For one, we’ve now established a more coherent type hierarchy by refactoring how we’re subclassing from the base SQLAlchemy type classes. We’ve also straightened up the naming of our types to follow the conventions suggested by SQLAlchemy, ensuring that the names of our SQL types in `types.py` are as closely aligned with the native database type names as possible. These changes, in combination with the efforts made to declutter the module namespace, have created a more purposeful design in the dialect’s type system that more clearly outlines the fundamental differences between the generic and SQL types. All in all, this brings the types of the dialect, and as a result the dialect as a whole, more to the level of what SQLAlchemy and its users expect. In terms of new functionalities, we’ve also implemented new data types: `PERIOD`, `BYTEINT`, `NUMBER`, and `BLOB`. With the addition of these new types, the Teradata dialect now supports all predefined data types offered by the Teradata database. While support for UDTs and CDTs is currently lacking, the Teradata dialect is now able to competently handle all work loads involving ANSI SQL types (and more) which should more than satisfy the use cases of most end users. Another notable change is that we’ve remapped a selection of Python types to corresponding Teradata types and overrode the literal processor of those types to correctly render literals as properly formatted SQL. This helps to ensure that literal values that interact with Teradata types within SQLAlchemy DDL statements or otherwise statements comprising literal bind parameters are correctly compiled. Apart from these higher level changes to the design of all Teradata types, efforts in the direction of affording more type-specific features for a number of data types were also pursued. For example, the interval types have been fitted with more flexible bind/result processing behavior and binary data types produce a warning when data may get truncated upon insertion. With all these fairly substantial modifications to the type system, more thorough tests have also been written for existing and newly added types alike to ensure that they all behave as expected when instantiated, printed, compiled, reflected, etc. In particular, extra care was taken to check that all types and their corresponding attributes are correctly interpreted by the DBAPI and backend, and that table reflection correctly retrieves the type attribute information specified by the user when they first constructed the types. Another big part of the changes coming with this version of the dialect deals with expression adaptation and type inference. We wanted to give users the ability to accurately infer the resulting types of `BinaryExpression`s involving arithmetic operations over Teradata data types and SQLAlchemy gives us the ability to do so through its expression adaptation mechanism. By overriding key methods in this process, we were able to embed custom Teradata logic into our type classes such that arithmetic operations over them evaluate to `BinaryExpression`s with a `.type` field which correctly identifies the type of the resulting expression. Of course, what we’ve mentioned so far only just cover the main improvements introduced with this release. Indeed, many other bug fixes and minor features were added with this patch as well-—details of which can be found in the many PRs leading up to this release.

We should note that two wiki pages have been written for the repo to go along with the two core changes of this release. We recommend reading them to learn more about the details involving SQLAlchemy data types and expression adaptation. The article on data types broadly covers the SQLAlchemy type API and its conventions and is geared towards end users. On the other hand, the expression adaptation article goes in depth into the implementation details regarding the mechanics of expression adaptation and type inference and is very much developer-oriented. Both are linked below:

1. [Data Types](https://github.com/wiskojo/sqlalchemy-teradata/wiki/Data-Types)
2. [Expression Adaptation](https://github.com/wiskojo/sqlalchemy-teradata/wiki/Expression-Adaptation)

**Implemented enhancements:**

- Implement get\_select\_precolumns and limit\_clause [\#17](https://github.com/Teradata/sqlalchemy-teradata/issues/17)
- Implement get\_columns [\#16](https://github.com/Teradata/sqlalchemy-teradata/issues/16)
- Update create\_connect\_args with database argument [\#14](https://github.com/Teradata/sqlalchemy-teradata/issues/14)
- Add support for BLOB types [\#12](https://github.com/Teradata/sqlalchemy-teradata/issues/12)
- implement get\_primary\_keys [\#11](https://github.com/Teradata/sqlalchemy-teradata/issues/11)
- Teradata expression adaptation [\#59](https://github.com/Teradata/sqlalchemy-teradata/pull/59) ([wiskojo](https://github.com/wiskojo))
- Refactor get\_columns\(\) [\#58](https://github.com/Teradata/sqlalchemy-teradata/pull/58) ([wiskojo](https://github.com/wiskojo))
- Implementation of the Teradata Period types [\#50](https://github.com/Teradata/sqlalchemy-teradata/pull/50) ([wiskojo](https://github.com/wiskojo))

**Fixed bugs:**

- Types need to implement \_\_str\_\_ [\#53](https://github.com/Teradata/sqlalchemy-teradata/issues/53)
- visit mechanism for BYTEINT [\#49](https://github.com/Teradata/sqlalchemy-teradata/issues/49)
- Fix get\_\* methods in dialect [\#15](https://github.com/Teradata/sqlalchemy-teradata/issues/15)

**Closed issues:**

- Incorrect syntax generated when creating index of a column [\#46](https://github.com/Teradata/sqlalchemy-teradata/issues/46)
- User doesn't have access for metadata callbacks [\#43](https://github.com/Teradata/sqlalchemy-teradata/issues/43)
- Missing reserved words [\#41](https://github.com/Teradata/sqlalchemy-teradata/issues/41)
- Use pyodbc instead of PyTd?  [\#40](https://github.com/Teradata/sqlalchemy-teradata/issues/40)
- Lock Row For Access When Using SQLAlchemy on Teradata [\#39](https://github.com/Teradata/sqlalchemy-teradata/issues/39)
- Issue createing Sqlalchemy engine - "Can't load plugin: sqlalchemy.dialects:teradata" [\#37](https://github.com/Teradata/sqlalchemy-teradata/issues/37)
- Wrong version number and outdated PyPI version [\#36](https://github.com/Teradata/sqlalchemy-teradata/issues/36)
- Can't populate Teradata timestamps via Flask-SQLAlchemy calls [\#34](https://github.com/Teradata/sqlalchemy-teradata/issues/34)
- Can't load teradata dialect [\#33](https://github.com/Teradata/sqlalchemy-teradata/issues/33)
- Error in reflecting a table with columns TIMESTAMP\(0\) [\#31](https://github.com/Teradata/sqlalchemy-teradata/issues/31)
- How can I specify "database" in create\_engine? [\#30](https://github.com/Teradata/sqlalchemy-teradata/issues/30)
- Unable to resolve type of columns in a view [\#27](https://github.com/Teradata/sqlalchemy-teradata/issues/27)
- Error while connecting to Teradata using Python [\#26](https://github.com/Teradata/sqlalchemy-teradata/issues/26)
- engine `table\_names` always returns empty list [\#25](https://github.com/Teradata/sqlalchemy-teradata/issues/25)
- Column type DECIMAL\(N,1\) maps to Decimal\('NaN'\) [\#24](https://github.com/Teradata/sqlalchemy-teradata/issues/24)
- Cannot connect from windows - teradata 'DRIVER\_NOT\_FOUND'?? [\#21](https://github.com/Teradata/sqlalchemy-teradata/issues/21)
- Add session methods in the dialect [\#19](https://github.com/Teradata/sqlalchemy-teradata/issues/19)
- limit\_clause should include \*\*kwargs in it's function arguments [\#6](https://github.com/Teradata/sqlalchemy-teradata/issues/6)
- Add "timestamp" to reserved keywords  [\#4](https://github.com/Teradata/sqlalchemy-teradata/issues/4)
- Schema meaning in dialect implementation [\#3](https://github.com/Teradata/sqlalchemy-teradata/issues/3)
- Status of Implementation of sqlalchemy.engine.default.DefaultDialect [\#2](https://github.com/Teradata/sqlalchemy-teradata/issues/2)
- AUTHMECH=LDAP [\#1](https://github.com/Teradata/sqlalchemy-teradata/issues/1)

**Merged pull requests:**

- Modify `test\_td\_ddl` unit test to work with Python 2 [\#69](https://github.com/Teradata/sqlalchemy-teradata/pull/69) ([wiskojo](https://github.com/wiskojo))
- Refactor tests to enforce dictionary column ordering [\#68](https://github.com/Teradata/sqlalchemy-teradata/pull/68) ([wiskojo](https://github.com/wiskojo))
- Refactor normalize\_name\(\) [\#66](https://github.com/Teradata/sqlalchemy-teradata/pull/66) ([wiskojo](https://github.com/wiskojo))
- Literal processing and type adaptation [\#63](https://github.com/Teradata/sqlalchemy-teradata/pull/63) ([wiskojo](https://github.com/wiskojo))
- Compile binary operator refactor [\#57](https://github.com/Teradata/sqlalchemy-teradata/pull/57) ([wiskojo](https://github.com/wiskojo))
- Truncation warning for Teradata binary types [\#56](https://github.com/Teradata/sqlalchemy-teradata/pull/56) ([wiskojo](https://github.com/wiskojo))
- Type resolution refactor [\#55](https://github.com/Teradata/sqlalchemy-teradata/pull/55) ([wiskojo](https://github.com/wiskojo))
- Fix for \_\_str\_\_ on Teradata types causing exception [\#54](https://github.com/Teradata/sqlalchemy-teradata/pull/54) ([wiskojo](https://github.com/wiskojo))
- Type system rework [\#52](https://github.com/Teradata/sqlalchemy-teradata/pull/52) ([wiskojo](https://github.com/wiskojo))
- Changes to the implementation of the Teradata Interval types [\#51](https://github.com/Teradata/sqlalchemy-teradata/pull/51) ([wiskojo](https://github.com/wiskojo))
- Added tests, refactored TDCreateTablePostfix, and a few other things... [\#48](https://github.com/Teradata/sqlalchemy-teradata/pull/48) ([wiskojo](https://github.com/wiskojo))
- Test Suite corrections [\#32](https://github.com/Teradata/sqlalchemy-teradata/pull/32) ([RemiTurpaud](https://github.com/RemiTurpaud))
- View Reflection [\#29](https://github.com/Teradata/sqlalchemy-teradata/pull/29) ([RemiTurpaud](https://github.com/RemiTurpaud))
- Add full text of MIT license [\#28](https://github.com/Teradata/sqlalchemy-teradata/pull/28) ([theianrobertson](https://github.com/theianrobertson))
- add MERGE as detected data changing operation [\#23](https://github.com/Teradata/sqlalchemy-teradata/pull/23) ([everilae](https://github.com/everilae))
- Dialect methods implementation [\#9](https://github.com/Teradata/sqlalchemy-teradata/pull/9) ([mrbungie](https://github.com/mrbungie))
- Fix proposal for \#3 [\#8](https://github.com/Teradata/sqlalchemy-teradata/pull/8) ([mrbungie](https://github.com/mrbungie))
- Fixes limit\_clause signature problems [\#7](https://github.com/Teradata/sqlalchemy-teradata/pull/7) ([mrbungie](https://github.com/mrbungie))
- Added timestamp to ReservedKeywords [\#5](https://github.com/Teradata/sqlalchemy-teradata/pull/5) ([mrbungie](https://github.com/mrbungie))


### Changes in Version 0.0.6 (Released: July 6, 2016)
The [original repository](https://github.com/sandan/sqlalchemy-teradata) has moved under the Teradata organization :tada::confetti_ball::chart_with_upwards_trend:
 The initial implementation includes the following changes:
* `An implementation of TeradataDialect and the various compilers`
* `Implement various generic types in the TeradataTypeCompiler and dialect specific types`
* `Various tests for the types and usage of the dialect`

The majority of the development in the beginning is squashed into the following commit:
   * [add tests, compiler impls, type impls](https://github.com/Teradata/sqlalchemy-teradata/commit/def0489f6f75bbfaf6012027394e78747a3941fc)

\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*
