import re
import decimal


from sqlalchemy.dialects import postgresql
from sqlalchemy import schema
from sqlalchemy.sql import expression



class Partition:
    def __init__(self, column_name):
        pass
    def clause(self):
        raise NotImplementedError('abstract method must be overridden')


class ListPartition(Partition):
    # mapping is a dict of str to value
    def __init__(self, column_name, mapping, subpartitions = []):
        self.column_name = column_name
        self.mapping = mapping
        self.subpartitions = subpartitions

    def clause(self):
        return ' LIST PARTITION TEST '


class RangePartition(Partition):
    def __init__(self, column_name, start, end, every, subpartitions = []):
        self.column_name = column_name
        self.start = start
        self.end = end
        self.every = every
        self.subpartitions = subpartitions


    def clause(self):
        return ' RANGE PARTITION TEST '

class ListSubpartition(Partition):
    def __init__(self, column_name, mapping):
        self.column_name = column_name
        self.mapping = mapping

    def clause(self):
        return ' LIST SUBPARTITION TEST '

class RangeSubpartition(Partition):
    def __init__(self, column_name, start, end, every):
        self.column_name = column_name
        self.start = start
        self.end = end
        self.every = every

    def clause(self):
        return ' RANGE SUBPARTITION TEST '


def format_partition_value(type_, value):
    '''
    Cast an input value based on the SQL type. This is done so
    that we can use the repr function to insert the value into
    RAW SQL

    Args:
        type_: an sqlalchemy type instance e.x. TEXT()
        value: value to cast

    Returns:
        str: the value cast to its python equivalent and represented as a string

    Note:
        uses double dollar sign quoted strings for strings containing single quotes https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING
    '''
    if type_.python_type in [int, float, decimal.Decimal]:
        return str(type_.python_type(value))
    elif type_.python_type == str:
        if '\'' in value:
            return '$${}$$'.format(value)
        else:
            return '\'{}\''.format(value)
    elif type_.python_type == bool:
        if str(value).lower() in ['t', 'true', '1']:
            return 'TRUE'
        elif str(value).lower() in ['f', 'false', '0']:
            return 'FALSE'
    raise NotImplementedError('unsupported type ({}) for the given value ({}) in hawq has not been implemented'.format(
        type_.python_type, value
    ))


