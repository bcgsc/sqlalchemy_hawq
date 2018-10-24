import re


from sqlalchemy.dialects import postgresql
from sqlalchemy import schema
from sqlalchemy.sql import expression


from .ddl import HawqDDLCompiler


class HawqDialect(postgresql.psycopg2.PGDialect_psycopg2):
    construct_arguments = [
        (schema.Table, {
            'partition_by': None,
            'inherits': None,
            'distributed_by': None,
            'appendonly': None,
            'orientation': None,
            'compresstype': None,
            'compresslevel': None,
            'on_commit': None,
            'tablespace': None
        })
    ]
    ddl_compiler = HawqDDLCompiler

