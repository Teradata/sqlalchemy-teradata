import os
import re

from setuptools import setup

v = open(os.path.join(os.path.dirname(__file__), 'sqlalchemy_teradata', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

setup(name='sqlalchemy_teradata',
      version=VERSION,
      description="Teradata dialect for SQLAlchemy",
      keywords='Teradata',
      author='Mike Wilson',
      author_email='mike.wilson@teradata.com',
      license='Commercial/Private',
      packages=['sqlalchemy_teradata'],
      include_package_data=True,
      # test_suite="nose.collector",
      zip_safe=False,
      entry_points={
         'sqlalchemy.dialects': [
               'tdalchemy = sqlalchemy_teradata.dialect:TeradataDialect'
              ]
        }
      )
