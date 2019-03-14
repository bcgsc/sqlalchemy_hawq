"""
Extends postgres dialect to support Apache HAWQ db ddl and dml.
"""

from sqlalchemy.dialects import registry


registry.register('hawq', 'hawq_sqlalchemy.dialect', 'HawqDialect')
registry.register('hawq+psycopq2', 'hawq_sqlalchemy.dialect', 'HawqDialect')
