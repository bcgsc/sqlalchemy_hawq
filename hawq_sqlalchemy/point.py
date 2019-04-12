'''
Defines Hawq point type for use by Sqlalchemy
'''
import re


from sqlalchemy.types import UserDefinedType


class SQLAlchemyHawqException(Exception):
    """
    Custom exception name for Hawq-Sqlalchemy package
    """


class Point(UserDefinedType):
    """
    Wrapper for a 2-element tuple.
    The Point type is available in HAWQ db and postgres DBAPI, but not in SQLAlchemy.
    """
    def get_col_spec(value):  # pylint: disable=no-self-argument
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
            if value is None:
                return None
            try:
                val1, val2 = value
                if val1 is None or val2 is None:
                    if val1 is not None or val2 is not None:
                        raise SQLAlchemyHawqException('Both values must be non-null or no data will be saved for Point({})'.format(value))
                    return None
                return str(value)
            except (ValueError, TypeError):
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
