'''
Data definition language support for the Apache Hawq database
'''
import re


from sqlalchemy.types import UserDefinedType


class SQLAlchemyHawqException(Exception):
    """
    Custom exception name for Hawq-Sqlalchemy package
    """
    pass


class Point(UserDefinedType):
    """
    Wrapper for a 2-element tuple.
    The Point type is available in HAWQ db and postgres DBAPI, but not in SQLAlchemy.
    """
    def get_col_spec(value):
        """
        Returns type name.
        get_col_spec must be overridden when implementing a custom class.
        """
        return "POINT"

    def bind_processor(self, dialect):
        """
        Returns a method to convert the tuple input to a its SQL string.
        """
        def process(value):
            try:
                val1, val2 = value
                return str(value)
            except:
                raise SQLAlchemyHawqException('Unexpected input type for Point ({})'.format(value))
        return process

    def result_processor(self, dialect, coltype):
        """
        Transforms the SQL string into a Python tuple.
        Point((float x),(float y)) -> (float x, float y)
        """
        def process(value):
            if value is None:
                return None
            match = re.match(r'^\((?P<x>\d+(\.\d+)?),(?P<y>\d+(\.\d+)?)\)$', value)
            if match:
                return (float(match.group('x')), float(match.group('y')))
            raise SQLAlchemyHawqException('Failed to get Point value from SQL ({})'.format(value))
        return process
