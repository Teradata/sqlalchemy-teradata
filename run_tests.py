from sqlalchemy.dialects import registry
from sqlalchemy.testing import runner

# registry.register("teradata", "sqlalchemy_teradata.pyodbc", "teradataDialect_pyodbc")
registry.register("tdalchemy", "sqlalchemy_teradata.dialect", "TeradataDialect")
# registry.register("teradata.pyodbc", "sqlalchemy_teradata.pyodbc", "teradataDialect_pyodbc")


runner.main()
