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
    Partial implementation of the Postgresql Point class, available in
    HAWQ dbs but not in Sqlalchemy.

    Provides storage and retrieval functions but does not include
    comparison or math ops.
    """

    def get_col_spec(value):
        """
        Returns type name.
        get_col_spec must be overridden when implementing a custom class.
        """
        return "POINT"

    def bind_func(value):
        """
        Takes an input value and converts it to a SQL Point string if possible.
        If the value is a string, does NOT check for correctness.
        Raises a custom exception including the value it failed on.
        """
        if (isinstance(value, dict)
                and 'x' in value and 'y' in value
                and value['x'] is not None and value['y'] is not None):
            return "({},{})".format(value['x'], value['y'])
        if isinstance(value, str):
            return value
        raise SQLAlchemyHawqException('Failed to cast value ({}) to Point type'.format(value))

    def bind_processor(self, dialect):
        """
        Returns a method to convert the value input in Python
        to its SQL representation.
        """
        return Point.bind_func

    def bind_expression(self, bindvalue):
        """
        Returns a Python Point object with its x and y values set
        and its 'value' value converted to its SQL representation.
        """
        bindvalue.value = Point.bind_func(bindvalue.value)
        return bindvalue

    def result_processor(self, dialect, coltype):
        """
        Returns a Python representation of a SQL Point value.
        Point((float x),(float y)) -> [float x, float y]
        """
        def process(value):
            if value is None:
                return None
            match = re.match(r'^\((\d+(\.\d+)?),(\d+(\.\d+)?)\)$', value)
            if match:
                lng, lat = value[1:-1].split(',')  # '(135.00,35.00)' => ('135.00', '35.00')
                return (float(lng), float(lat))
            raise SQLAlchemyHawqException('Failed to get Point value from SQL ({})'.format(value))
        return process
