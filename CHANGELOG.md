# Change Log

## [Unreleased](https://github.com/Teradata/sqlalchemy-teradata/tree/HEAD)

**Closed issues:**

- limit\_clause should include \*\*kwargs in it's function arguments [\#6](https://github.com/Teradata/sqlalchemy-teradata/issues/6)
- Add "timestamp" to reserved keywords  [\#4](https://github.com/Teradata/sqlalchemy-teradata/issues/4)
- Schema meaning in dialect implementation [\#3](https://github.com/Teradata/sqlalchemy-teradata/issues/3)
- AUTHMECH=LDAP [\#1](https://github.com/Teradata/sqlalchemy-teradata/issues/1)

**Merged pull requests:**

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
