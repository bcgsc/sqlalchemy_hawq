import sqlalchemy
from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing import exclusions

class Requirements(SuiteRequirements):
    @property
    def returning(self):
        return exclusions.closed()

    @property
    def cross_schema_fk_reflection(self):
        return exclusions.closed()

    @property
    def order_by_col_from_union(self):
        return exclusions.closed()

    @property
    def independent_connections(self):
        return exclusions.closed()

    @property
    def table_reflection(self):
        return exclusions.closed()

    
    @property
    def implicitly_named_constraints(self):
        """target database must apply names to unnamed constraints."""
        return exclusions.closed()

    @property
    def parens_in_union_contained_select_w_limit_offset(self):
        return exclusions.closed()

    @property
    def parens_in_union_contained_select_wo_limit_offset(self):
        return exclusions.closed()

    

    @property
    def reflects_pk_names(self):
        return exclusions.open()