'''
Data definition language support for the Apache Hawq database
'''
from sqlalchemy.dialects import postgresql
from sqlalchemy import schema


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
            raise ValueError(
                'orientation ({}) must be one of {}'.format(
                    table_opts['orientation'], {'ROW', 'PARQUET'}
                )
            )
        with_statement['orientation'] = table_opts['orientation'].upper()

    compresstype_values = {'ZLIB', 'SNAPPY', 'GZIP', 'NONE'}
    if table_opts['compresstype'] is not None:
        if table_opts['compresstype'] not in compresstype_values:
            raise ValueError(
                'compresstype ({}) must be one of {}'.format(
                    table_opts['compresstype'], compresstype_values
                )
            )
        with_statement['compresstype'] = table_opts['compresstype']

    if table_opts['compresslevel'] is not None:
        if table_opts['compresslevel'] < 0 or table_opts['compresslevel'] > 9:
            raise ValueError(
                'compresslevel ({}) must be an integer between 0-9'.format(
                    table_opts['compresslevel']
                )
            )
        with_statement['compresslevel'] = table_opts['compresslevel']

    if table_opts['bucketnum'] is not None:
        if table_opts['distributed_by'] is None:
            raise ValueError("""bucketnum is only valid when used with 'distributed_by'""")
        with_statement['bucketnum'] = table_opts['bucketnum']

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
                inherits = (inherits,)
            table_opts.append(
                '\nINHERITS ({}) '.format(', '.join(self.preparer.quote(name) for name in inherits))
            )

        table_opts.append(with_clause(pg_opts))

        if pg_opts['on_commit']:
            on_commit = {'PRESERVE ROWS', 'DELETE ROWS', 'DROP'}
            if pg_opts['on commit'].upper() not in on_commit:
                raise ValueError('Invalid option for on_commit {}'.format(pg_opts['on_commit']))
            table_opts.append('\n ON COMMIT {}'.format(pg_opts['on_commit'].upper()))

        if pg_opts['tablespace']:
            tablespace_name = pg_opts['tablespace']
            table_opts.append('\n TABLESPACE {}'.format(self.preparer.quote(tablespace_name)))

        if pg_opts['distributed_by']:
            table_opts.append('\nDISTRIBUTED BY ({})'.format(pg_opts['distributed_by']))

        if pg_opts['partition_by']:
            table_opts.append('\n' + partition_clause(table, pg_opts['partition_by']))

        return ''.join(table_opts)

    def create_table_constraints(self, table, _include_foreign_key_constraints=None):
        '''
        HAWQ only supports check constraints. Ignore any other constraints
        '''
        constraints = [
            c for c in table._sorted_constraints if isinstance(c, schema.CheckConstraint)
        ]
        retval = ''
        for constraint in constraints:
            if (constraint._create_rule is None or constraint._create_rule(self)) and not (
                self.dialect.supports_alter and getattr(constraint, 'use_alter', False)
            ):
                for item in self.process(constraint):
                    if item is not None:
                        retval = retval + ', \n    ' + item
        return retval
