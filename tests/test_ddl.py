import decimal


import pytest
from sqlalchemy.dialects import postgresql


from hawq_sqlalchemy.ddl import format_partition_value


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
