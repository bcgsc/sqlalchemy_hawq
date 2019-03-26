import sqlalchemy
from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing import exclusions
from sqlalchemy.engine.url import URL


class Requirements(SuiteRequirements):
    @property
    def returning(self):
        return exclusions.closed()

    @property
    def table_reflection(self):
        return exclusions.closed()

    @property
    def index_reflection(self):
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
    def ctes_with_update_delete(self):
        return exclusions.closed()


    @property
    def schema_reflection(self):
        return exclusions.closed()

    @property
    def view_reflection(self):
        return exclusions.closed()

    @property
    def foreign_key_constraint_reflection(self):
        return exclusions.closed()


    @property
    def implicitly_named_constraints(self):
        return exclusions.closed()

    @property
    def parens_in_union_contained_select_w_limit_offset(self):
        return exclusions.closed()

    @property
    def parens_in_union_contained_select_wo_limit_offset(self):
        return exclusions.closed()

    

    @property
    def reflects_pk_names(self):
        return exclusions.closed()


    ### ------- HAWQ-SPECIFIC PROPERTIES -------- ###

    @property
    def delete_append_only_statement(self):
        return exclusions.closed()

    @property
    def delete_append_only_statement_in_teardown(self):
        return exclusions.closed()

    @property
    def select_for_update_share(self):
        return exclusions.closed()

    @property
    def update_append_only_statement(self):
        return exclusions.closed()

    @property
    def test_schema_exists(self):
        return exclusions.closed()
