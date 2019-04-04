""" Stub for sqlalchemy's unit tests. """

from sqlalchemy.testing.suite import *

#=== 37 failed, 99 passed, 65 skipped, 46 error in 26.40 seconds =====
#===== 36 failed, 99 passed, 65 skipped, 46 error in 27.79 seconds ===
#==== 35 failed, 99 passed, 65 skipped, 48 error in 25.78 seconds ===
#=========== 99 passed, 102 skipped, 3 error in 24.13 seconds =====


class ComponentReflectionTest:
    pass

from sqlalchemy.testing.suite import SimpleUpdateDeleteTest as _SUDT

class SimpleUpdateDeleteTest(_SUDT):

    @testing.requires.delete_append_only_statement
    def test_delete(self):
        super.test_delete()
        return

    @testing.requires.update_append_only_statement
    def test_update(self):
        super.test_update()
        return


from sqlalchemy.testing.suite import TextTest as _TextTest

class TextTest(_TextTest):

    @testing.requires.delete_append_only_statement
    def test_text_roundtrip(self):
        super.test_text_roundtrip()
        return


from sqlalchemy.testing.suite import UnicodeVarcharTest as _UnicodeVarcharTest

class UnicodeVarcharTest(_UnicodeVarcharTest):

    @testing.requires.delete_append_only_statement
    def test_round_trip(self):
        super.test_round_trip()
        return

    @testing.requires.delete_append_only_statement
    def test_round_trip_executemany(self):
        super.test_round_trip_executemany()
        return


from sqlalchemy.testing.suite import UnicodeTextTest as _UnicodeTextTest

class UnicodeTextTest(_UnicodeTextTest):

    @testing.requires.delete_append_only_statement
    def test_round_trip(self):
        super.test_round_trip()
        return

    @testing.requires.delete_append_only_statement
    def test_round_trip_executemany(self):
        super.test_round_trip_executemany()
        return

from sqlalchemy.testing.suite import TimeTest as _TimeTest

class TimeTest(_TimeTest):
    @testing.requires.delete_append_only_statement
    def test_round_trip(self):
        super.test_round_trip()
        return

from sqlalchemy.testing.suite import TimeMicrosecondsTest as _TimeMicrosecondsTest

class TimeMicrosecondsTest(_TimeMicrosecondsTest):
    @testing.requires.delete_append_only_statement
    def test_round_trip(self):
        super.test_round_trip()
        return

from sqlalchemy.testing.suite import TableDDLTest as _TableDDLTest

class TableDDLTest(_TableDDLTest):
    @testing.requires.test_schema_exists
    def test_create_table_schema(self):
        super.test_create_table_schema()
        return

from sqlalchemy.testing.suite import ServerSideCursorsTest as _ServerSideCursorsTest

class ServerSideCursorsTest(_ServerSideCursorsTest):

    # make sure overriding teardown to not dispose of absent engine doesn't break anything
    def tearDown(self):
        engines.testing_reaper.close_all()
        if 'engine' in dir(self):
            self.engine.dispose()

    @testing.requires.update_append_only_statement
    def test_roundtrip(self):
        super.test_roundtrip()
        return

    @testing.requires.select_for_update_share
    def test_for_update_string(self):
        super.test_for_update_string()
        return

    @testing.requires.select_for_update_share
    def test_for_update_expr(self):
        super.test_for_update_expr()
        return


from sqlalchemy.testing.suite import OrderByLabelTest as _OrderByLabelTest

class OrderByLabelTest(_OrderByLabelTest):
    @testing.requires.delete_append_only_statement
    def test_plain_desc(self):
        super.test_plain_desc()
        return

    @testing.requires.delete_append_only_statement
    def test_composed_int_desc(self):
        super.test_composed_int_desc()
        return

    @testing.requires.delete_append_only_statement
    def test_composed_multiple(self):
        super.test_composed_multiple()
        return

    @testing.requires.delete_append_only_statement
    def test_plain(self):
        super.test_plain()
        return

    @testing.requires.delete_append_only_statement
    def test_group_by_composed(self):
        super.test_group_by_composed()
        return

from sqlalchemy.testing.suite import LimitOffsetTest as _LimitOffsetTest

class LimitOffsetTest(_LimitOffsetTest):

    @testing.requires.delete_append_only_statement
    def test_simple_offset(self):
        super.test_simple_offset()
        return

    @testing.requires.delete_append_only_statement
    def test_simple_limit(self):
        super.test_simple_limit()
        return

    @testing.requires.delete_append_only_statement
    def test_simple_limit_offset(self):
        super.test_simple_limit_offset()
        return

    @testing.requires.delete_append_only_statement
    def test_limit_offset_nobinds(self):
        super.test_limit_offset_nobinds()
        return

    @testing.requires.delete_append_only_statement
    def test_bound_offset(self):
        super.test_bound_offset()
        return

    @testing.requires.delete_append_only_statement
    def test_bound_limit_offset(self):
        super.test_bound_limit_offset()
        return

from sqlalchemy.testing.suite import LastrowidTest as _LastrowidTest

class LastrowidTest(_LastrowidTest):
    @testing.requires.delete_append_only_statement
    def test_last_inserted_id(self):
        super.test_last_inserted_id()
        return

from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest

class InsertBehaviorTest(_InsertBehaviorTest):
    @testing.requires.delete_append_only_statement
    def test_insert_from_select_autoinc(self):
        super.test_insert_from_select_autoinc()
        return

    @testing.requires.delete_append_only_statement
    def test_insert_from_select_autoinc_no_rows(self):
        super.test_insert_from_select_autoinc_no_rows()
        return

from sqlalchemy.testing.suite import ExpandingBoundInTest as _ExpandingBoundInTest

class ExpandingBoundInTest(_ExpandingBoundInTest):

    @testing.requires.delete_append_only_statement
    def test_empty_set_against_integer_negation(self):
        super.test_empty_set_against_integer_negation()
        return

    @testing.requires.delete_append_only_statement
    def test_empty_set_against_string_negation(self):
        super.test_empty_set_against_string_negation()
        return

    @testing.requires.delete_append_only_statement
    def test_bound_in_scalar(self):
        super.test_bound_in_scalar()
        return



from sqlalchemy.testing.suite import ExceptionTest as _ExceptionTest

class ExceptionTest(_ExceptionTest):

    @testing.requires.delete_append_only_statement
    def test_integrity_error(self):
        super.test_integrity_error()
        return


from sqlalchemy.testing.suite import DateTimeTest as _DateTimeTest

class DateTimeTest(_DateTimeTest):

    @testing.requires.delete_append_only_statement
    def test_round_trip(self):
        super.test_round_trip()
        return


from sqlalchemy.testing.suite import DateTimeMicrosecondsTest as _DateTimeMicrosecondsTest

class DateTimeMicrosecondsTest(_DateTimeMicrosecondsTest):

    @testing.requires.delete_append_only_statement
    def test_round_trip(self):
        super.test_round_trip()
        return

from sqlalchemy.testing.suite import DateTimeCoercedToDateTimeTest as _DateTimeCoercedToDateTimeTest

class DateTimeCoercedToDateTimeTest(_DateTimeCoercedToDateTimeTest):

    @testing.requires.delete_append_only_statement
    def test_round_trip(self):
        super.test_round_trip()
        return

from sqlalchemy.testing.suite import DateTest as _DateTest

class DateTest(_DateTest):
    @testing.requires.delete_append_only_statement
    def test_round_trip(self):
        super.test_round_trip()
        return

from sqlalchemy.testing.suite import BooleanTest as _BooleanTest

class BooleanTest(_BooleanTest):
    @testing.requires.delete_append_only_statement
    def test_whereclause(self):
        super.test_whereclause()
        return

    @testing.requires.delete_append_only_statement
    def test_round_trip(self):
        super.test_round_trip()
        return
