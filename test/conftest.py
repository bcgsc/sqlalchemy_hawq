"""
The entry point for the sqlalchemy test suite.

Command line args are added and the test collector
is modified to check them.
 """


from sqlalchemy.dialects import registry
from sqlalchemy.testing import config
from sqlalchemy.testing.plugin import pytestplugin
from sqlalchemy.testing.plugin import plugin_base
from sqlalchemy.testing.plugin.pytestplugin import *


registry.register('hawq', 'hawq_sqlalchemy.dialect', 'HawqDialect')
registry.register('hawq+psycopq2', 'hawq_sqlalchemy.dialect', 'HawqDialect')


def pytest_addoption(parser):
    """
    Adds custom args, then calls the sqlalchemy pytest_addoption method to handle the rest
    """
    parser.addoption("--custom-only",
                     action="store_true",
                     default=False,
                     help="run only hawq_sqlalchemy custom tests")
    parser.addoption("--unit-only",
                     action="store_true",
                     default=False,
                     help="run only hawq_sqlalchemy custom unit tests")
    parser.addoption("--sqla-only",
                     action="store_true",
                     default=False,
                     help="run only the sqlalchemy test suite")
    pytestplugin.pytest_addoption(parser)


def pytest_pycollect_makeitem(collector, name, obj):
    """
    Decides which tests not to run, then passes the rest of the work to
    the sqla method with the same name
    """

    if inspect.isclass(obj) and plugin_base.want_class(obj):

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

        # only run the sqla test suite
        if config.options.sqla_only:
            if collector.name != 'test_suite.py':
                return[]

        return pytestplugin.pytest_pycollect_makeitem(collector, name, obj)
    return pytestplugin.pytest_pycollect_makeitem(collector, name, obj)
