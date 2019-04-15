"""
Extends postgres dialect to support Apache HAWQ db ddl and dml.
"""

from sqlalchemy.dialects import registry


registry.register('hawq', 'sqlalchemy_hawq.dialect', 'HawqDialect')
registry.register('hawq+psycopq2', 'sqlalchemy_hawq.dialect', 'HawqDialect')
