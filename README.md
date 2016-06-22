 Dialect for SQLAlchemy
 ===============================

 SQLAlchemy is a database toolkit that provides an abstraction over databases. It allows you to interact with relational databases using an object relational mapper or through a pythonic sql rendering engine known as the core. 

 Read the documentation and more: http://www.sqlalchemy.org/

 The Teradata Dialect is an implementation of SQLAlchemy's Dialect System. It implements various classes that are specific to interacting with the teradata dbapi, construction of sql specific to Teradata, and more. The project is still in an incubation phase. See test/usage_test for how the dialect is used for the core expression language api.

Design Principles
=================

    * Have a simple setup process and a minimal learning curve
    * Provide a simple core that is modular and extensible
    * Be an easy way to interact with the database out of the box

Get Involved
============

    * We welcome your contributions in: Documentation, Bug Reporting, Tests, and Code (Features & Bug Fixes)
    * You can contribute to our documentation by going to our github wiki.
    * All code submissions are done through pull requests. 

Tests
======
The project uses pytest. You can run pytest in the sqlalchemy-teradata directory with the ```py.test```command. Typical usage:

```python
py.test -s test/*
py.test -s test/test_suite.py
py.test -s test/test_suite.py::TestClass
py.test -s test/test_suite.py::TestClass::test_func
```

see the [pytest docs](http://pytest.org/latest/contents.html#toc) for more info

See Also
========

[PyTd](https://github.com/Teradata/PyTd): the DB API 2.0 implementation found in the teradata module
[sqlalchemy_aster](https://github.com/KarolTx/sqlalchemy_aster): A SQLAlchemy dialect for aster

