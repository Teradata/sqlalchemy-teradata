from sqlalchemy import *
from sqlalchemy.dialects import registry

registry.register("teradata", "sqlalchemy_teradata.dialect", "TeradataDialect")
from sqlalchemy.testing.plugin.pytestplugin import *
