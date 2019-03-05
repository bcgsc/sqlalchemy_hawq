'''
Data definition language support for the Apache Hawq database
'''
import re


from sqlalchemy.dialects import postgresql
from sqlalchemy import schema
from sqlalchemy import types
from sqlalchemy.types import UserDefinedType
from sqlalchemy.exc import StatementError


from .partition import partition_clause


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
                 constraint._create_rule(self)
             )
             and (
                 not self.dialect.supports_alter or
                 not getattr(constraint, 'use_alter', False)
             )
            ) if p is not None
        )


class Point(UserDefinedType):

    """
    Partial implementation of the Postgresql Point class, available in
    HAWQ dbs but not in Sqlalchemy.

    Provides storage and retrieval functions but does not include
    comparison or math ops.
    """
    x = None
    y = None

    def __repr__(self):
        return "Point(%s,%s)" % self.x, self.y

    def get_col_spec(value):
        """
        Returns type name.
        get_col_spec must be overridden when implementing a custom class.
        """
        return "POINT"

    def __init__(self, dictvals=None):
        if isinstance(dictvals, dict):
            if dictvals['x'] is not None:
                self.x = dictvals['x']
            if dictvals['y'] is not None:
                self.y = dictvals['y']

    def ints_to_point(value):
        """
        Takes an input array of length 2 and outputs
        a SQL Point string.
        [x,y] -> Point((float x), (float y))
        """
        if isinstance(value, dict):
            return "(%s,%s)" % (value['x'], value['y'])
        if isinstance(value, str):
            return value
        raise StatementError(message='message='Failed to cast value ({}) to Point type'.format(value)')

    def bind_processor(self, dialect):
        """
        Returns a method to convert the value input in Python
        to its SQL representation.
        """
        return Point.ints_to_point

    def bind_expression(self, bindvalue):
        """
        Returns a Python Point object with its x and y values set
        and its 'value' value converted to its SQL representation.
        """
        if bindvalue.value['x'] is not None:
            self.x = bindvalue.value['x']
        if bindvalue.value['y'] is not None:
            self.y = bindvalue.value['y']
        bindvalue.value = Point.ints_to_point(bindvalue.value)
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
            raise StatementError(message='message='Failed to cast value ({}) to hawq_sqlalchemy Point type'.format(value)')
        return process
