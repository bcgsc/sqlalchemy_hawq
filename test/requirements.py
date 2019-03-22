import sqlalchemy
from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing import exclusions

class Requirement(SuiteRequirements):
    @property
    def returning(self):
        return exclusions.open()

    @property
    def cross_schema_fk_reflection(self):
        return exclusions.open()