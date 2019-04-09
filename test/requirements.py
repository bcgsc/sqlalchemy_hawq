"""
Imports and modifies SQLAlchemy's Requirements class.

Markers from this class can be used to switch marked tests
on and off, so that only the functionality the dialect
supports is tested.
"""
from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):
    """
    Changes the settings of some requirements to work with Hawq.
    Adds some requirements specific to Hawq.
    """
    @property
    def returning(self):
        """
        Hawq does not support a 'RETURNING' clause
        """
        return exclusions.closed()

    @property
    def cross_schema_fk_reflection(self):
        """
        Requested by sqla test suite for ComponentReflectionTest,
        even though ComponentReflectionTests are disabled.
        """
        return exclusions.closed()

    @property
    def order_by_col_from_union(self):
        """
        Requested by sqla test for CompoundSelectTest
        """
        return exclusions.open()

    @property
    def ctes_with_update_delete(self):
        """
        Hawq does not support update or delete on append-only tables
        """
        return exclusions.open()

    @property
    def duplicate_key_raises_integrity_error(self):
        """
        Hawq does not enforce primary key uniquing
        """
        return exclusions.closed()


    ### ------- HAWQ-SPECIFIC PROPERTIES -------- ###

    @property
    def delete_row_statement_for_append_only_table(self):
        """
        ProgrammingError('(psycopg2.ProgrammingError)
        Delete append-only table statement not supported yet\n')
        """
        return exclusions.closed()

    @property
    def select_for_update_share(self):
        """sqlalchemy.exc.NotSupportedError: (psycopg2.NotSupportedError)
        Cannot support select for update/share statement yet"""

        return exclusions.closed()

    @property
    def update_append_only_statement(self):
        """
        ProgrammingError('(psycopg2.ProgrammingError)
        Delete append-only table statement not supported yet\n')
        """
        return exclusions.closed()

    @property
    def test_schema_exists(self):
        """
        Depends on user to create test_schema in the target db.
        """
        return exclusions.closed()
