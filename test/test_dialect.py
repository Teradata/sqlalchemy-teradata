from sqlalchemy_teradata.dialect import TeradataDialect
from sqlalchemy.testing import fixtures

class TeradataDialectTest(fixtures.TestBase):
    def setup(self):
        self.dialect = TeradataDialect()

    def test_Attrs(self):
        assert hasattr(self.dialect, 'name')
