import decimal


import pytest
from sqlalchemy.dialects import postgresql


from hawq_sqlalchemy.ddl import format_ddl_value


class TestFormatDDLValue:
    def test_integer(self):
        result = format_ddl_value(postgresql.INTEGER(), 1)
        assert isinstance(result, int)

    def test_numeric(self):
        result = format_ddl_value(postgresql.NUMERIC(), 1)
        assert isinstance(result, decimal.Decimal)

    def test_boolean(self):
        result = format_ddl_value(postgresql.BOOLEAN(), 'f')
        assert result is False

        result = format_ddl_value(postgresql.BOOLEAN(), 'true')
        assert result is True

    def test_real(self):
        result = format_ddl_value(postgresql.REAL(), '1.1')
        assert isinstance(result, float)

    def test_float(self):
        result = format_ddl_value(postgresql.FLOAT(), '1.1')
        assert isinstance(result, float)

    def test_enum(self):
        result = format_ddl_value(postgresql.ENUM(), '1.1')
        assert isinstance(result, str)

    def test_varchar(self):
        result = format_ddl_value(postgresql.VARCHAR(), 'something')
        assert isinstance(result, str)

    def test_text(self):
        result = format_ddl_value(postgresql.TEXT(), 'something')
        assert isinstance(result, str)

    def test_char(self):
        result = format_ddl_value(postgresql.CHAR(), 's')
        assert isinstance(result, str)
