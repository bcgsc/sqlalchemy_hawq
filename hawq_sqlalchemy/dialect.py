from sqlalchemy.dialects import postgresql
from sqlalchemy import schema
from sqlalchemy.ext.compiler import compiles
from .ddl import HawqDDLCompiler
from sqlalchemy.sql.expression import (Delete)


class HawqDialect(postgresql.psycopg2.PGDialect_psycopg2):
    '''
    Main dialect class. Used by the engine to compile sql
    '''
    construct_arguments = [
        (schema.Table, {
            'partition_by': None,
            'inherits': None,
            'distributed_by': None,
            'bucketnum': None,
            'appendonly': None,
            'orientation': None,
            'compresstype': None,
            'compresslevel': None,
            'on_commit': None,
            'tablespace': None
        })
    ]
    ddl_compiler = HawqDDLCompiler
    name = 'hawq'

    def initialize(self, connection):
        super().initialize(connection)
        self.implicit_returning = False 

    @compiles(Delete, 'hawq')
    def visit_delete_statement(element, compiler, **kwargs):
        """
        Allows a version of the delete statement to get compiled - the version
        that is effectively the same as truncate.

        Any filters on the delete statement result in an Exception.
        """
        delete_stmt_table = compiler.process(element.table, asfrom=True, **kwargs)
        filters_tuple = element.get_children()
        if not filters_tuple:
            return 'TRUNCATE TABLE {}'.format(delete_stmt_table)
        else:
            raise NotImplementedError('Delete statement with filter clauses not implemented for Hawq')