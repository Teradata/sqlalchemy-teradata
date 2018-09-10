# Change Log

## [Unreleased](https://github.com/Teradata/sqlalchemy-teradata/tree/HEAD)

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



\* *This Change Log was automatically generated by [github_changelog_generator](https://github.com/skywinder/Github-Changelog-Generator)*