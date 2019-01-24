'''
Data definition language support for the Apache Hawq database
'''

import re
import decimal


from sqlalchemy.dialects import postgresql
from sqlalchemy import schema
from sqlalchemy.sql import expression



class Partition:
    def __init__(self, column_name):
        pass
    def clause(table):
        raise NotImplementedError('abstract method must be overridden')


class ListPartition(Partition):
    # mapping is a dict of str to value
    def __init__(self, column_name, mapping, level=1):
        return
    def clause(table):
        raise NotImplementedError('abstract method must be overridden')


class RangePartition(Partition):
    def __init__(self, column_name, start, end, every, level=1):
        return
    def clause(table):
        raise NotImplementedError('abstract method must be overridden')



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
        partition_by (tuple of str and dict by str): the column to partition on and the mapping of partition names to values

    Note:
        currently only supports partitioning a table by list not range

    Warning:
        the column_name must be the database column name and not the attribute name of the column for the declarative model

    Returns:
        str: the partition clause
    '''
    #print(partition_by)
    return "test"

"""     column_name, partitions = partition_by
    column = table.columns.get(column_name)
    if column is None:
        raise ValueError('Column ({}) to use for partitioning not found'.format(column_name))

    partition_statments = []
    for name, value in partitions.items():
        partition_statments.append('\tPARTITION {} VALUES ({}),'.format(
            valid_partition_name(name),
            format_partition_value(column.type, value)
        ))
    partition_statments.append('\tDEFAULT PARTITION other')
    statement = 'PARTITION BY LIST ({})\n(\n{}\n)'.format(
        column_name,
        '\n'.join(partition_statments)
    )
    return statement
 """

def with_clause(table_opts):
    '''
    Create the WITH clause for table DDL to indicates storage parameters

    Args:
        table_opts (dict): the dictionary of table specific arguments

    Returns:
        str: the with clause to follow CREATE TABLE
    '''
    with_statement = {}

    if table_opts['appendonly'] is not None:
        if not isinstance(table_opts['appendonly'], bool):
            raise ValueError('appendonly must be boolean')
        with_statement['appendonly'] = table_opts['appendonly']

    if table_opts['orientation'] is not None:
        if str(table_opts['orientation']).upper() not in {'ROW', 'PARQUET'}:
            raise ValueError('orientation ({}) must be one of {}'.format(
                table_opts['orientation'], {'ROW', 'PARQUET'}
            ))
        with_statement['orientation'] = table_opts['orientation'].upper()

    compresstype_values = {'ZLIB', 'SNAPPY', 'GZIP', 'NONE'}
    if table_opts['compresstype'] is not None:
        if table_opts['compresstype'] not in compresstype_values:
            raise ValueError('compresstype ({}) must be one of {}'.format(
                table_opts['compresstype'], compresstype_values
            ))
        with_statement['compresstype'] = table_opts['compresstype']

    if table_opts['compresslevel'] is not None:
        if table_opts['compresslevel'] < 0 or table_opts['compresslevel'] > 9:
            raise ValueError('compresslevel ({}) must be an integer between 0-9'.format(
                table_opts['compresslevel']
            ))
        with_statement['compresslevel'] = table_opts['compresslevel']

    if not with_statement:
        return ''
    with_statement = ', '.join(['{}={}'.format(k, v) for k, v in with_statement.items()])
    return '\nWITH ({})'.format(with_statement)



class HawqDDLCompiler(postgresql.base.PGDDLCompiler):
    '''
    override the default postgres DDL
    '''
    def visit_create_index(self, create):
        raise NotImplementedError('HAWQ does not support indexing')

    def visit_drop_index(self, drop):
        raise NotImplementedError('HAWQ does not support indexing')

    def post_create_table(self, table):
        '''
        statments to add after the table is created that still apply to the table creation
        '''
        table_opts = []
        pg_opts = table.dialect_options['hawq']

        inherits = pg_opts.get('inherits')
        if inherits is not None:
            if not isinstance(inherits, (list, tuple)):
                inherits = (inherits, )
            table_opts.append(
                '\nINHERITS ( ' +
                ', '.join(self.preparer.quote(name) for name in inherits) +
                ' )')

        table_opts.append(with_clause(pg_opts))

        if pg_opts['on_commit']:
            on_commit = {'PRESERVE ROWS', 'DELETE ROWS', 'DROP'}
            if pg_opts['on commit'].upper() not in on_commit:
                raise ValueError('Invalid option for on_commit {}'.format(pg_opts['on_commit']))
            table_opts.append(
                '\n ON COMMIT {}'.format(pg_opts['on_commit'].upper())
            )

        if pg_opts['tablespace']:
            tablespace_name = pg_opts['tablespace']
            table_opts.append(
                '\n TABLESPACE {}'.format(self.preparer.quote(tablespace_name))
            )

        if pg_opts['distributed_by']:
            table_opts.append('\nDISTRIBUTED BY ({})'.format(pg_opts['distributed_by']))

        if pg_opts['partition_by']:
            table_opts.append('\n' + partition_clause(table, pg_opts['partition_by']))

        return ''.join(table_opts)

    def create_table_constraints(self, table, _include_foreign_key_constraints=None):
        '''
        HAWQ only supports check constraints. Ignore any other constraints
        '''
        constraints = [c for c in table._sorted_constraints if isinstance(c, schema.CheckConstraint)]
        return ', \n\t'.join(
            p for p in
            (self.process(constraint)
                for constraint in constraints
                if (
                    constraint._create_rule is None or
                    constraint._create_rule(self))
                and (
                    not self.dialect.supports_alter or
                    not getattr(constraint, 'use_alter', False)
            )) if p is not None
        )

