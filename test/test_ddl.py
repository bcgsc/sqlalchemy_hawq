from sqlalchemy.dialects import postgresql
from sqlalchemy.testing.suite import fixtures
from sqlalchemy.testing import assert_raises


from sqlalchemy_hawq.partition import format_partition_value
from sqlalchemy_hawq.point import Point
from sqlalchemy_hawq.point import SQLAlchemyHawqException


class TestFormatPartitionValue(fixtures.TestBase):
    def test_integer(self):
        result = format_partition_value(postgresql.INTEGER(), 1)
        assert result == '1'

    def test_numeric(self):
        result = format_partition_value(postgresql.NUMERIC(), 1)
        assert result == '1'

    def test_boolean(self):
        result = format_partition_value(postgresql.BOOLEAN(), 'f')
        assert result == 'FALSE'

        result = format_partition_value(postgresql.BOOLEAN(), 'true')
        assert result == 'TRUE'

    def test_real(self):
        result = format_partition_value(postgresql.REAL(), '1.1')
        assert result == '1.1'

    def test_float(self):
        result = format_partition_value(postgresql.FLOAT(), '1.1')
        assert result == '1.1'

    def test_enum(self):
        result = format_partition_value(postgresql.ENUM(), '1.1')
        assert result == '\'1.1\''

    def test_varchar(self):
        result = format_partition_value(postgresql.VARCHAR(), 'something')
        assert result == '\'something\''

    def test_text(self):
        result = format_partition_value(postgresql.TEXT(), 'something')
        assert result == '\'something\''

    def test_char(self):
        result = format_partition_value(postgresql.CHAR(), 's')
        assert result == '\'s\''

    def test_escape_string(self):
        result = format_partition_value(postgresql.CHAR(), '\'s\'')
        assert result == '$$\'s\'$$'


class TestPointSQLConversion(fixtures.TestBase):
    def test_string_to_tuple_correct(self):
        func = Point.result_processor(1, 2, 3)
        assert func('(99,100)') == (99, 100)

    def test_string_to_tuple_incorrect(self):
        func = Point.result_processor(1, 2, 3)
        assert_raises(SQLAlchemyHawqException, func, '(99,)')

    def test_string_to_tuple_incorrect_2(self):
        func = Point.result_processor(1, 2, 3)
        assert_raises(SQLAlchemyHawqException, func, '(,99)')

    def test_string_to_tuple_incorrect_3(self):
        func = Point.result_processor(1, 2, 3)
        assert_raises(SQLAlchemyHawqException, func, '(,)')

    def test_string_to_tuple_incorrect_4(self):
        func = Point.result_processor(1, 2, 3)
        assert_raises(SQLAlchemyHawqException, func, '(a,b)')

    def test_none_to_none_result(self):
        func = Point.result_processor(1, 2, 3)
        assert func(None) is None

    def test_none_to_none_bind(self):
        func = Point.bind_processor(1, 2)
        assert func((None, None)) is None

    def test_none_to_none_bind_2(self):
        func = Point.bind_processor(1, 2)
        assert func(None) is None

    def test_tuple_to_string(self):
        func = Point.bind_processor(1, 2)
        assert func((1, 2)) == '(1, 2)'

    def test_tuple_to_string_incorrect(self):
        func = Point.bind_processor(1, 2)
        assert_raises(SQLAlchemyHawqException, func, (None, 2))

    def test_tuple_to_string_incorrect_2(self):
        func = Point.bind_processor(1, 2)
        assert_raises(SQLAlchemyHawqException, func, (2, None))
