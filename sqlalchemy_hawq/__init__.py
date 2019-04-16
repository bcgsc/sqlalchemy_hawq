"""
Extends postgres dialect to support Apache HAWQ db ddl and dml.
"""

from sqlalchemy.dialects import registry
import pkg_resources


registry.register('hawq', 'sqlalchemy_hawq.dialect', 'HawqDialect')
registry.register('hawq+psycopq2', 'sqlalchemy_hawq.dialect', 'HawqDialect')


__version__ = pkg_resources.require('sqlalchemy_hawq')[0].version
