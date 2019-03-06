import decimal


import pytest
from sqlalchemy.dialects import postgresql


from hawq_sqlalchemy.partition import format_partition_value
from hawq_sqlalchemy.point import Point
from hawq_sqlalchemy.point import SQLAlchemyHawqException


class TestFormatPartitionValue:
    def test_integer(self):
        result = format_partition_value(postgresql.INTEGER(), 1)
        assert '1' == result

    def test_numeric(self):
        result = format_partition_value(postgresql.NUMERIC(), 1)
        assert '1' == result

    def test_boolean(self):
        result = format_partition_value(postgresql.BOOLEAN(), 'f')
        assert 'FALSE' == result

        result = format_partition_value(postgresql.BOOLEAN(), 'true')
        assert 'TRUE' == result

    def test_real(self):
        result = format_partition_value(postgresql.REAL(), '1.1')
        assert '1.1' == result

    def test_float(self):
        result = format_partition_value(postgresql.FLOAT(), '1.1')
        assert '1.1' == result

    def test_enum(self):
        result = format_partition_value(postgresql.ENUM(), '1.1')
        assert '\'1.1\'' == result

    def test_varchar(self):
        result = format_partition_value(postgresql.VARCHAR(), 'something')
        assert '\'something\'' == result

    def test_text(self):
        result = format_partition_value(postgresql.TEXT(), 'something')
        assert '\'something\'' == result

    def test_char(self):
        result = format_partition_value(postgresql.CHAR(), 's')
        assert '\'s\'' == result

    def test_escape_string(self):
        result = format_partition_value(postgresql.CHAR(), '\'s\'')
        assert '$$\'s\'$$' == result


class TestPointSQLConversion:
    def test_string_to_tuple_correct(self):
        func = Point.result_processor(1, 2, 3)
        assert func('(99,100)') == (99,100)

    def test_string_to_tuple_incorrect(self):
        func = Point.result_processor(1, 2, 3)
        with pytest.raises(SQLAlchemyHawqException):
            assert func('(99,)') == (99,)

    def test_string_to_tuple_incorrect_2(self):
        func = Point.result_processor(1, 2, 3)
        with pytest.raises(SQLAlchemyHawqException):
            assert func('(,99)') == (99,)

    def test_string_to_tuple_incorrect_3(self):
        func = Point.result_processor(1, 2, 3)
        with pytest.raises(SQLAlchemyHawqException):
            assert func('(,)') == (99,)

    def test_string_to_tuple_incorrect_4(self):
        func = Point.result_processor(1, 2, 3)
        with pytest.raises(SQLAlchemyHawqException):
            assert func('(a,b)') == (99,)

    def test_none_to_none_result(self):
        func = Point.result_processor(1, 2, 3)
        assert func(None) == None

    def test_none_to_none_bind(self):
        func = Point.bind_processor(1, 2)
        assert func((None,None)) == None

    def test_none_to_none_bind_2(self):
        func = Point.bind_processor(1, 2)
        assert func(None) == None

    def test_tuple_to_string(self):
        func = Point.bind_processor(1, 2)
        assert func((1,2)) == '(1, 2)'

    def test_tuple_to_string_incorrect(self):
        func = Point.bind_processor(1,2)
        with pytest.raises(SQLAlchemyHawqException):
            assert func((None,2)) == '(1, 2)'

    def test_tuple_to_string_incorrect_2(self):
        func = Point.bind_processor(1,2)
        with pytest.raises(SQLAlchemyHawqException):
            assert func((2,None)) == '(1, 2)'