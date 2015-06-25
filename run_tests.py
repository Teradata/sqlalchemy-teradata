from sqlalchemy.dialects import registry

registry.register("teradata", "sqlalchemy_teradata.pyodbc", "teradataDialect_pyodbc")
registry.register("teradata.pyodbc", "sqlalchemy_teradata.pyodbc", "teradataDialect_pyodbc")

from sqlalchemy.testing import runner

runner.main()
