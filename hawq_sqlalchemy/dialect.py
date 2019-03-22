from sqlalchemy.dialects import postgresql
from sqlalchemy import schema


from .ddl import HawqDDLCompiler


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
        super(postgresql.psycopg2.PGDialect_psycopg2, self).initialize(connection)
        self.implicit_returning = False 
