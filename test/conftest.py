import pytest


from sqlalchemy.dialects import registry
from sqlalchemy.testing.plugin.pytestplugin import *


registry.register('hawq', 'hawq_sqlalchemy.dialect', 'HawqDialect')
registry.register('hawq+psycopq2', 'hawq_sqlalchemy.dialect', 'HawqDialect')



