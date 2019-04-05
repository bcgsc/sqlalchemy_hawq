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

    @testing.requires.delete_row_statement_for_append_only_table
    def test_delete(self):
        _SUDT.test_delete(self)
        return

    @testing.requires.update_append_only_statement
    def test_update(self):
        _SUDT.test_update(self)
        return


from sqlalchemy.testing.suite import TextTest as _TextTest

class TextTest(_TextTest):

    @testing.requires.truncate_table
    def test_text_roundtrip(self):
        _TextTest.test_text_roundtrip(self)
        return


from sqlalchemy.testing.suite import UnicodeVarcharTest as _UnicodeVarcharTest

class UnicodeVarcharTest(_UnicodeVarcharTest):

    @testing.requires.truncate_table
    def test_round_trip(self):
        _UnicodeVarcharTest.test_round_trip(self)
        return

    @testing.requires.truncate_table
    def test_round_trip_executemany(self):
        _UnicodeVarcharTest.test_round_trip_executemany(self)
        return


from sqlalchemy.testing.suite import UnicodeTextTest as _UnicodeTextTest

class UnicodeTextTest(_UnicodeTextTest):

    @testing.requires.truncate_table
    def test_round_trip(self):
        _UnicodeTextTest.test_round_trip(self)
        return

    @testing.requires.truncate_table
    def test_round_trip_executemany(self):
        _UnicodeTextTest.test_round_trip_executemany(self)
        return


from sqlalchemy.testing.suite import TimeTest as _TimeTest

class TimeTest(_TimeTest):
    @testing.requires.truncate_table
    def test_round_trip(self):
        _TimeTest.test_round_trip(self)
        return


from sqlalchemy.testing.suite import TimeMicrosecondsTest as _TimeMicrosecondsTest

class TimeMicrosecondsTest(_TimeMicrosecondsTest):
    @testing.requires.truncate_table
    def test_round_trip(self):
        _TimeMicrosecondsTest.test_round_trip(self)
        return


from sqlalchemy.testing.suite import TableDDLTest as _TableDDLTest

class TableDDLTest(_TableDDLTest):
    @testing.requires.test_schema_exists
    def test_create_table_schema(self):
        _TableDDLTest.test_create_table_schema(self)
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
        _ServerSideCursorsTest.test_roundtrip(self)
        return

    @testing.requires.select_for_update_share
    def test_for_update_string(self):
        _ServerSideCursorsTest.test_for_update_string(self)
        return

    @testing.requires.select_for_update_share
    def test_for_update_expr(self):
        _ServerSideCursorsTest.test_for_update_expr(self)
        return


from sqlalchemy.testing.suite import OrderByLabelTest as _OrderByLabelTest

class OrderByLabelTest(_OrderByLabelTest):
    @testing.requires.truncate_table
    def test_plain_desc(self):
        _OrderByLabelTest.test_plain_desc(self)
        return

    @testing.requires.truncate_table
    def test_composed_int_desc(self):
        _OrderByLabelTest.test_composed_int_desc(self)
        return

    @testing.requires.truncate_table
    def test_composed_multiple(self):
        _OrderByLabelTest.test_composed_multiple(self)
        return

    @testing.requires.truncate_table
    def test_plain(self):
        _OrderByLabelTest.test_plain(self)
        return

    @testing.requires.truncate_table
    def test_group_by_composed(self):
        _OrderByLabelTest.test_group_by_composed(self)
        return


from sqlalchemy.testing.suite import LimitOffsetTest as _LimitOffsetTest

class LimitOffsetTest(_LimitOffsetTest):
    @testing.requires.truncate_table
    def test_simple_offset(self):
        _LimitOffsetTest.test_simple_offset(self)
        return

    @testing.requires.truncate_table
    def test_simple_limit(self):
        _LimitOffsetTest.test_simple_limit(self)
        return

    @testing.requires.truncate_table
    def test_simple_limit_offset(self):
        _LimitOffsetTest.test_simple_limit_offset(self)
        return

    @testing.requires.truncate_table
    def test_limit_offset_nobinds(self):
        _LimitOffsetTest.test_limit_offset_nobinds(self)
        return

    @testing.requires.truncate_table
    def test_bound_offset(self):
        _LimitOffsetTest.test_bound_offset(self)
        return

    @testing.requires.truncate_table
    def test_bound_limit_offset(self):
        _LimitOffsetTest.test_bound_limit_offset(self)
        return


from sqlalchemy.testing.suite import LastrowidTest as _LastrowidTest

class LastrowidTest(_LastrowidTest):
    @testing.requires.truncate_table
    def test_last_inserted_id(self):
        _LastrowidTest.test_last_inserted_id(self)
        return


from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest

class InsertBehaviorTest(_InsertBehaviorTest):
    @testing.requires.truncate_table
    def test_insert_from_select_autoinc(self):
        _InsertBehaviorTest.test_insert_from_select_autoinc(self)
        return

    @testing.requires.truncate_table
    def test_insert_from_select_autoinc_no_rows(self):
        _InsertBehaviorTest.test_insert_from_select_autoinc_no_rows(self)
        return


from sqlalchemy.testing.suite import ExpandingBoundInTest as _ExpandingBoundInTest

class ExpandingBoundInTest(_ExpandingBoundInTest):
    @testing.requires.truncate_table
    def test_empty_set_against_integer_negation(self):
        _ExpandingBoundInTest.test_empty_set_against_integer_negation(self)
        return

    @testing.requires.truncate_table
    def test_empty_set_against_string_negation(self):
        _ExpandingBoundInTest.test_empty_set_against_string_negation(self)
        return

    @testing.requires.truncate_table
    def test_bound_in_scalar(self):
        _ExpandingBoundInTest.test_bound_in_scalar(self)
        return


from sqlalchemy.testing.suite import ExceptionTest as _ExceptionTest

class ExceptionTest(_ExceptionTest):
    @testing.requires.delete_row_statement_for_append_only_table
    def test_integrity_error(self):
        _ExceptionTest.test_integrity_error(self)
        return


from sqlalchemy.testing.suite import DateTimeTest as _DateTimeTest

class DateTimeTest(_DateTimeTest):
    @testing.requires.truncate_table
    def test_round_trip(self):
        _DateTimeTest.test_round_trip(self)
        return


from sqlalchemy.testing.suite import DateTimeMicrosecondsTest as _DateTimeMicrosecondsTest

class DateTimeMicrosecondsTest(_DateTimeMicrosecondsTest):
    @testing.requires.truncate_table
    def test_round_trip(self):
        _DateTimeMicrosecondsTest.test_round_trip(self)
        return


from sqlalchemy.testing.suite import DateTimeCoercedToDateTimeTest as _DateTimeCoercedToDateTimeTest

class DateTimeCoercedToDateTimeTest(_DateTimeCoercedToDateTimeTest):
    @testing.requires.truncate_table
    def test_round_trip(self):
        _DateTimeCoercedToDateTimeTest.test_round_trip(self)
        return


from sqlalchemy.testing.suite import DateTest as _DateTest

class DateTest(_DateTest):
    @testing.requires.truncate_table
    def test_round_trip(self):
        _DateTest.test_round_trip(self)
        return


from sqlalchemy.testing.suite import BooleanTest as _BooleanTest

class BooleanTest(_BooleanTest):
    def tearDown(self):
        print('init to win it')
        import pdb; pdb.set_trace()
        print('test')



    @testing.requires.truncate_table
    def test_whereclause(self):

        _BooleanTest.test_whereclause(self)
        return

    @testing.requires.truncate_table
    def test_round_trip(self):
        _BooleanTest.test_round_trip(self)
        return
