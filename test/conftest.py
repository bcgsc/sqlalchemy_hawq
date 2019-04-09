import argparse


import pytest


from sqlalchemy.dialects import registry
from sqlalchemy.testing import config
from sqlalchemy.testing.plugin import pytestplugin
from sqlalchemy.testing.plugin import plugin_base
from sqlalchemy.testing.plugin.pytestplugin import *


registry.register('hawq', 'hawq_sqlalchemy.dialect', 'HawqDialect')
registry.register('hawq+psycopq2', 'hawq_sqlalchemy.dialect', 'HawqDialect')


def pytest_addoption(parser):
    parser.addoption("--custom-only", action="store_true", default=False, help="run only hawq_sqlalchemy custom tests")
    parser.addoption("--unit-only", action="store_true", default=False, help="run only hawq_sqlalchemy custom tests")
    pytestplugin.pytest_addoption(parser)


def pytest_pycollect_makeitem(collector, name, obj):
    """
    Decides which tests not to run, then passes the rest of the work to 
    the sqla method with the same name
    """
    if inspect.isclass(obj) and plugin_base.want_class(obj):

        """ Custom checks """
        # only run custom tests, not sqla_tests
        if config.options.custom_only:
            if collector.name == 'test_suite.py':
                return []

        # only run custom unit tests
        if config.options.unit_only:
            if collector.name == 'test_suite.py':
                return []
            if collector.name == 'test_live_connection.py':
                return []

        return pytestplugin.pytest_pycollect_makeitem(collector, name, obj)
    return pytestplugin.pytest_pycollect_makeitem(collector, name, obj)
