import sqlalchemy_views as sv

from sqlalchemy import Table

class View(Table):
    definition = None

    def __init__(name, metadata, source_definition):
        definition = source_definition
        Table.__init__(name, metadata)
