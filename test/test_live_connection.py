"""
Defines tests that require a live db connection.
"""
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer
from sqlalchemy.exc import StatementError
from sqlalchemy.testing.suite import fixtures
from sqlalchemy.testing import assert_raises
from sqlalchemy.orm import Session


from sqlalchemy_hawq.point import Point


class TestWithLiveConnection(fixtures.DeclarativeMappedTest):

    # ensures this test class is run when option --backend-only is specified
    __backend__ = True

    @classmethod
    def setup_classes(cls):
        Base = cls.DeclarativeBasic

        class MockTable(fixtures.ComparableEntity, Base):
            __tablename__ = 'mocktable'
            id = Column("id", Integer, primary_key=True)
            test = Column("test", Integer)
            __table_args__ = {'schema': 'test_schema'}

        class PointTable(fixtures.ComparableEntity, Base):
            __tablename__ = 'point_table'
            id = Column('id', Integer, primary_key=True)
            ptest = Column('ptest', Point)
            __table_args__ = {'schema': 'test_schema'}

    def test_inner_live_setup(self):
        """
        Checks that the live connection works, ie
        that a table can be made and data entered and queried.

        This is also a test of disable-implicit-returning clause:
        if the 'returning' clause is attached, an exception will be
        thrown; if no sql is sent, the test will fail.
        """
        mocktable = self.classes.MockTable
        session = Session()
        session.add(mocktable(test=5))
        session.commit()
        session.close()

        res = session.query(mocktable).all()
        session.close()
        expected = (res[0].test, len(res))

        assert expected == (5, 1)

    def test_no_implicit_returning_clause(self):
        """
        Checks that the dialect is passing 'implicit_returning'=False
        to the engine, and no 'returning' clause is added to the sql.
        """

        MockTable = self.classes.MockTable
        ins = MockTable.__table__.insert().values(test=5).compile()
        expected = str(ins)
        assert expected == 'INSERT INTO test_schema.mocktable (id, test) VALUES (%(id)s, %(test)s)'

    def test_point_type_insert_select(self):
        """
        Checks that point type data can be inserted and selected.
        """
        point_table = self.classes.PointTable
        session = Session()
        session.add(point_table(id=1, ptest=(14, 201)))
        session.commit()
        session.close()

        res = session.query(point_table).filter_by(id=1)
        session.close()

        expected = (14, 201)
        assert expected == res[0].ptest

    def test_point_type_insert_select_nonetype(self):
        """
        Checks that none-type point type data is inserted and retrieved correctly.
        """
        point_table = self.classes.PointTable
        session = Session()
        session.add(point_table(id=2, ptest=None))
        session.commit()
        session.close()

        res = session.query(point_table).filter_by(id=2)
        session.close()

        expected = None
        assert expected == res[0].ptest

    def test_point_type_insert_select_none_in_tuple_type(self):
        """
        Checks that none-type point type data is inserted and retrieved correctly.
        """
        point_table = self.classes.PointTable
        session = Session()
        session.add(point_table(id=3, ptest=(None, None)))
        session.commit()
        session.close()

        res = session.query(point_table).filter_by(id=3)
        session.close()

        expected = None
        assert expected == res[0].ptest

    def test_point_type_insert_select_mixed_type(self):
        """
        Checks that exception is raised when mixed none and non-none-type data is passed
        """
        point_table = self.classes.PointTable
        session = Session()

        def func():
            """
            Ensure session is closed before exception is raised
            """
            try:
                session.add(point_table(id=4, ptest=(1, None)))
                session.commit()
                session.close()
            except Exception as e:
                session.close()
                raise e
        assert_raises(StatementError, func)

    def test_point_type_insert_select_mixed_type_2(self):
        """
        Checks that exception is raised when mixed none and non-none-type data is passed
        """
        point_table = self.classes.PointTable
        session = Session()

        def func():
            """
            Ensure session is closed before exception is raised
            """
            try:
                session.add(point_table(id=5, ptest=(None, 1)))
                session.commit()
                session.close()
            except Exception as e:
                session.close()
                raise e
        assert_raises(StatementError, func)
