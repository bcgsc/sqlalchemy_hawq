from sqlalchemy.dialects import registry


registry.register('hawq', 'hawq_sqlalchemy.dialect', 'HawqDialect')
registry.register('hawq+psycopq2', 'hawq_sqlalchemy.dialect', 'HawqDialect')