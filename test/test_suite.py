"""
Stub for sqlalchemy's unit tests.
Imports and modifies some test subsuites.

Disables tests of sqlalchemy functionality that Hawq dialect does not support.
"""


from sqlalchemy.testing.suite import *
from sqlalchemy.testing.suite import SimpleUpdateDeleteTest as _SimpleUpdateDeleteTest
from sqlalchemy.testing.suite import TableDDLTest as _TableDDLTest
from sqlalchemy.testing.suite import ServerSideCursorsTest as _ServerSideCursorsTest




class ComponentReflectionTest():
    """
    Every test in the ComponentReflectionTest suite relies on indexing.
    Hawq does not support indexes, so skip all tests.

    TODO: Check if they can be rewritten to run without indexes.
    """


#TODO: make sure the tests can't be called if other requires are placed on them in sqla

class SimpleUpdateDeleteTest(_SimpleUpdateDeleteTest):
    @testing.requires.delete_row_statement_for_append_only_table
    def test_delete(self):
        _SimpleUpdateDeleteTest.test_delete(self)

    @testing.requires.update_append_only_statement
    def test_update(self):
        _SimpleUpdateDeleteTest.test_update(self)


class TableDDLTest(_TableDDLTest):
    @testing.requires.test_schema_exists
    def test_create_table_schema(self):
        _TableDDLTest.test_create_table_schema(self)


class ServerSideCursorsTest(_ServerSideCursorsTest):
    def tearDown(self):
        """
        Overrides parent teardown method to prevent calling dispose
        on engine that does not exist if test is skipped.
        """
        engines.testing_reaper.close_all()
        if 'engine' in dir(self):
            self.engine.dispose()

    @testing.requires.update_append_only_statement
    def test_roundtrip(self):
        _ServerSideCursorsTest.test_roundtrip(self)

    @testing.requires.select_for_update_share
    def test_for_update_string(self):
        _ServerSideCursorsTest.test_for_update_string(self)

    @testing.requires.select_for_update_share
    def test_for_update_expr(self):
        _ServerSideCursorsTest.test_for_update_expr(self)
