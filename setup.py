import os
import re

from setuptools import setup

v = open(os.path.join(os.path.dirname(__file__), 'sqlalchemy_teradata', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.md')


setup(name='sqlalchemy_teradata',
      version=VERSION,
      description="Teradata dialect for SQLAlchemy",
      long_description=open(readme).read(),
      classifiers=[
      'Development Status :: 3 - Alpha',
      'Environment :: Console',
      'Intended Audience :: Developers',
      'Programming Language :: Python',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: Implementation :: CPython',
      'Topic :: Database :: Front-Ends',
      ],
      keywords='Teradata',
      author='Mike Wilson',
      author_email='mike.wilson@teradata.com',
      license='Commercial/Private',
      packages=['sqlalchemy_teradata'],
      include_package_data=True,
      tests_require=['nose >= 0.11'],
      test_suite="nose.collector",
      zip_safe=False,
      entry_points={
         'sqlalchemy.dialects': [
              'teradata = sqlalchemy_teradata.pyodbc:teradataDialect_pyodbc',
              'teradata.pyodbc = sqlalchemy_teradata.pyodbc:teradataDialect_pyodbc',
              ]
        }
)
