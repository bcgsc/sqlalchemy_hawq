""" Partition and Subpartition class definitions and supporting funcs.

Defines Range- and List- Partition and Subpartition classes
for use as the values of key 'hawq_partition_by'.

"""
import re
import decimal


class Partition:
    """ Base class.

    Implements methods for checking partition name
    and assembling subpartition clauses.

    Args:
        column_name (str): The name of the column to partition on.
        subpartitions (array of subpartitions, optional): Any subpartitions to include
            in the partition plan.

    """

    def __init__(self, column_name, subpartitions=[]):
        self.column_name = column_name
        self.subpartitions = subpartitions

    def partition_column(self, table):
        """Checks if the column_name provided on initialization is actually in the table
        to be partitioned.

        Args:
            the table to partition.

        Returns:
            column (Column): the column to partition on.

        Raises:
            ValueError if column to partition on is not found in the table.

        """
        column = table.columns.get(self.column_name)
        if column is None:
            raise ValueError('Column ({}) to use for partitioning not found'.format(self.column_name))
        return column

    def get_subpartition_statements(self, table):
        """Assembles the clauses generated by each subpartition into a list.

        Args:
            table(:obj:) the table to partition.

        Returns:
            subpartition_statements(:obj:`list` of str): the assembled
                subpartition statements.

        """
        subpartition_statements = []
        for item in self.subpartitions:
            subpartition_statements.append('\n' + item.clause(table))
        return subpartition_statements

    def clause(self, table):
        """Base version of func that returns the assembled clause."""
        raise NotImplementedError('abstract method must be overridden')


class ListPartition(Partition):
    """ A class representing a list-style top-level partition.

    Initializes with values specific to list-style partitions and implements
    methods to return a list-style partition clause.

    Inherits from Partition.

    Attributes:
        mapping (:obj:`dict` of str): maps column value defining a partition
            to the name of that partition.

    """

    #  mapping is a dict of str to value
    def __init__(self, column_name, mapping, subpartitions=[]):
        self.column_name = column_name
        self.mapping = mapping
        self.subpartitions = subpartitions

    def get_partition_statements(self, column, partition_level=''):
        """ Assembles a partition clause.

        Args:
            column(Column): the column to partition on.
            partition_level(str, optional): either '' or 'SUB'. Prepended to
            'PARTITION' in strs.

        Returns:
            partition_statements(`list` of str): a list of str defining the partitions
            for this column.

        """
        partition_statements = []
        for name, value in self.mapping.items():
            partition_statements.append("""    {}PARTITION {} VALUES ({}),""".format(
                partition_level,
                valid_partition_name(name),
                format_partition_value(column.type, value)
            ))
        return partition_statements

    def clause(self, table):
        """ Assembles the partition clause.

        Args:
            table: the table to partition.

        Returns:
            statement(str): the partition clause for this table.

         """
        column = self.partition_column(table)
        partition_statements = self.get_partition_statements(column)
        subpartition_statements = self.get_subpartition_statements(table)

        statement = """PARTITION BY LIST ({}){}
(
{}
    DEFAULT PARTITION other
)""".format(
    self.column_name, """
""".join(subpartition_statements), """
""".join(partition_statements)
        )
        return statement


class ListSubpartition(ListPartition):
    """ Overrides a parent method in order to provide a modified partition clause.

        Inherits from ListPartition.

    """

    def clause(self, table):
        """ Returns a clause defining a list-type subpartition.

        Args:
            table: the table being partitioned

        Returns:
            statement(str): the clause for this subpartition.
         """

        column = self.partition_column(table)
        partition_statements = self.get_partition_statements(column, 'SUB')

        statement = """    SUBPARTITION BY LIST ({})
    SUBPARTITION TEMPLATE
    (
    {}
        DEFAULT SUBPARTITION other
    )""".format(
        self.column_name,
        """
    """.join(partition_statements)
        )
        return statement


class RangePartition(Partition):
    """ A class representing a range-style top-level partition.

    Initializes with values specific to range-style partitions and implements
    methods to return a range-style partition clause.

    Inherits from Partition.

    Attributes:
        start(int): the beginning of the range to partition on
        end(int): the end of the range to partition on
        every(int): the size of the subranges defining each partition

    """

    def __init__(self, column_name, start, end, every, subpartitions=[]):
        self.column_name = column_name
        self.start = start
        self.end = end
        self.every = every
        self.subpartitions = subpartitions

    def clause(self, table):
        """ Assembles the partition clause.

        Args:
            table: the table to partition.

        Returns:
            statement(str): the partition clause for this table.

         """

        self.partition_column(table)  # raises an error if column_name is invalid
        subpartition_statements = self.get_subpartition_statements(table)

        statement = """PARTITION BY RANGE ({}){}
(
    START ({}) END ({}) EVERY ({}),
    DEFAULT PARTITION extra
)""".format(
    self.column_name,
    """
""".join(subpartition_statements),
    self.start, self.end, self.every
        )
        return statement


class RangeSubpartition(RangePartition):
    """ Overrides a parent method in order to provide a modified partition clause.

    Inherits from RangePartition.

    """

    def clause(self, table):
        """ Returns a clause defining a range-type subpartition.

        Args:
            table: the table being partitioned

        Returns:
            statement(str): the clause for this subpartition.
         """

        self.partition_column(table)  # raises an error if column_name is invalid

        statement = """    SUBPARTITION BY RANGE ({})
    SUBPARTITION TEMPLATE
    (
        START ({}) END ({}) EVERY ({}),
        DEFAULT SUBPARTITION extra
    )""".format(
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
        uses double dollar sign quoted strings for strings containing single quotes
         https://www.postgresql.org/docs/current/static/sql-syntax-lexical.html#SQL-SYNTAX-DOLLAR-QUOTING
    '''
    if type_.python_type in [int, float, decimal.Decimal]:
        return str(type_.python_type(value))
    if type_.python_type == str:
        if '\'' in value:
            return '$${}$$'.format(value)
        return '\'{}\''.format(value)
    if type_.python_type == bool:
        if str(value).lower() in ['t', 'true', '1']:
            return 'TRUE'
        elif str(value).lower() in ['f', 'false', '0']:
            return 'FALSE'
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
        partition_by (Partition):
            the Range- or List- partition object consisting of column name,
            partition args, and subpartition array

    Note:
        currently does not support partitioning by range on date

    Warning:
        the column_name must be the database column name and not the attribute name of
        the column for the declarative model

    Returns:
        str: the partition clause
    '''
    return partition_by.clause(table)
