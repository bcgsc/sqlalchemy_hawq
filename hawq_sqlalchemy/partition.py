import re
import decimal


from sqlalchemy.dialects import postgresql
from sqlalchemy import schema
from sqlalchemy.sql import expression



class Partition:
    def __init__(self, column_name):
        pass

    def partition_column(self, table):
        column = table.columns.get(self.column_name)
        if column is None:
            raise ValueError('Column ({}) to use for partitioning not found'.format(self.column_name))
        return column
        
    def get_subpartition_statements(self, table):
        subpartition_statements = []
        for item in self.subpartitions:
            subpartition_statements.append('\n' + item.clause(table))
        return subpartition_statements

    def clause(self, table):
        raise NotImplementedError('abstract method must be overridden')





class ListPartition(Partition):
    # mapping is a dict of str to value
    def __init__(self, column_name, mapping, subpartitions = []):
        self.column_name = column_name
        self.mapping = mapping
        self.subpartitions = subpartitions

    def get_partition_statements(self, column, partition_level=''):
        partition_statements = []
        for name, value in self.mapping.items():
            partition_statements.append('\t{}PARTITION {} VALUES ({}),'.format(
                partition_level,
                valid_partition_name(name),
                format_partition_value(column.type, value)
             ))
        return partition_statements

    def clause(self, table):
        column = self.partition_column(table)
        partition_statements = self.get_partition_statements(column)
        subpartition_statements = self.get_subpartition_statements(table)

        statement = 'PARTITION BY LIST ({})\
        {}\
        \n(\
        \n{}\
        \n\tDEFAULT PARTITION other\
        \n)'.format(
            self.column_name,
            '\n'.join(subpartition_statements),
            '\n'.join(partition_statements)
        )
        return statement



class ListSubpartition(ListPartition):
    def clause(self, table):
        column = self.partition_column(table)
        partition_statements = self.get_partition_statements(column, 'SUB')

        statement = '\tSUBPARTITION BY LIST ({})\
        \n\tSUBPARTITION TEMPLATE\
        \n\t(\
        \n\t{}\
        \n\t\tDEFAULT SUBPARTITION other\
        \n\t)'.format(
            self.column_name,
            '\n\t'.join(partition_statements)
        )
        return statement



class RangePartition(Partition):
    def __init__(self, column_name, start, end, every, subpartitions = []):
        self.column_name = column_name
        self.start = start
        self.end = end
        self.every = every
        self.subpartitions = subpartitions

    def clause(self, table):
        self.partition_column(table) # raises an error if column_name is invalid
        subpartition_statements = self.get_subpartition_statements(table)

        statement = 'PARTITION BY RANGE ({})\
        {}\
        \n(\
        \n\tSTART ({}) END ({}) EVERY ({}),\
        \n\tDEFAULT PARTITION extra\
        \n)'.format(
            self.column_name,
            '\n'.join(subpartition_statements),
            self.start, self.end, self.every
        )
        return statement



class RangeSubpartition(RangePartition):
    def clause(self, table):
        self.partition_column(table) # raises an error if column_name is invalid

        statement = '\tSUBPARTITION BY RANGE ({})\
        \n\tSUBPARTITION TEMPLATE\
        \n\t(\
        \n\t\tSTART ({}) END ({}) EVERY ({}),\
        \n\t\tDEFAULT SUBPARTITION extra\
        \n\t)'.format(
            self.column_name,
            self.start, self.end, self.every
        )
        return statement




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

def valid_partition_name(name):
    '''
    Checks that a partition name is word characters only (to avoid injection)

    Args:
        name (str): name of the partition

    Returns:
        str: the input name

    Raises:
        ValueError: when an invalid partition name is input
    '''
    if not re.match(r'^[a-z]\w+$', str(name), re.IGNORECASE):
        raise ValueError('invalid partition name ){})'.format(name))
    else:
        return name


def partition_clause(table, partition_by):
    '''
    Create the partition clause for when a partition is defined on a HAWQ table

    Args:
        table (sqlalchemy.schema.Table): the table being partitioned
        partition_by (tuple of str and dict by str): the Range- or List- partition object consisting of column name,
            partition args, and subpartition array

    Note:
        currently does not support partitioning by range on date

    Warning:
        the column_name must be the database column name and not the attribute name of the column for the declarative model

    Returns:
        str: the partition clause
    '''
    return partition_by.clause(table)




