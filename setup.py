from os import path
from setuptools import setup

setup(
    name='sqlalchemy_teradata',
    version='0.0.6',
    description="Teradata dialect for SQLAlchemy",
    classifiers=[
                      'Development Status :: 3 - Alpha',
                      'Environment :: Console',
                      'Intended Audience :: Developers',
                      'Programming Language :: Python',
                      'Programming Language :: Python :: 2.7',
                      'Programming Language :: Python :: Implementation :: CPython',
                      'Topic :: Database :: Front-Ends',
                ],
    keywords='Teradata SQLAlchemy',
    author='Mark Sandan',
    author_email='mark.sandan@teradata.com',
    license='Commercial/Private',
    packages=['sqlalchemy_teradata'],
    include_package_data=True,
    tests_require=['pytest >= 2.5.2'],
    install_requires=['sqlalchemy', 'teradata'],
    entry_points={
                'sqlalchemy.dialects': [
                           'teradata = sqlalchemy_teradata.dialect:TeradataDialect',
                                           ]
                }
)
