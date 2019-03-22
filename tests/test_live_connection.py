"""
Defines pytest tests the require a live db connection.
At the experimental stage.
"""

import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer
from sqlalchemy.exc import StatementError


from hawq_sqlalchemy.point import Point


@pytest.fixture(scope='class')
def base():
    """
    Returns a single declarative base for use by all tests in a class
    """
    return declarative_base()


@pytest.fixture(scope='class')
def PointTable(base, test_engine, schemaname):
    """
    Defines the PointTable object type and table for use by all tests in a class
    """
    class PointTable(base):
        __tablename__ = 'point_table'
        id = Column('id', Integer, primary_key=True)
        ptest = Column('ptest', Point)
        __table_args__ = {'schema': schemaname}
    base.metadata.create_all(test_engine)
    return PointTable


@pytest.fixture(scope='class')
def MockTable(base, test_engine, schemaname):
    """
    Defines the MockTable object type and table for use by all tests in a class
    """
    class MockTable(base):
        __tablename__ = 'mocktable'
        id = Column('id', Integer, primary_key=True)
        test = Column('test', Integer)
        __table_args__ = {'schema': schemaname}
    base.metadata.create_all(test_engine)
    return MockTable


class TestWithLiveConnection:
    """
    A group of tests requiring a live connection.
    """
    def test_live_setup(self, SessionFactory, MockTable):
        """
        Checks that the live connection works, ie
        that a table can be made and data entered and queried.
        """
        session = SessionFactory()
        session.add(MockTable(id=99, test=5))
        session.commit()
        session.close()

        session2 = SessionFactory()
        res = session2.query(MockTable).all()
        expected = res[0].test
        session2.commit()
        session2.close()

        assert expected == 5


    def test_point_type_insert_select(self, SessionFactory, PointTable):
        """
        Checks that point type data can be inserted and selected.
        """

        session = SessionFactory()
        session.add(PointTable(id=1, ptest=(14, 201)))
        session.commit()
        session.close()

        session2 = SessionFactory()
        res = session2.query(PointTable).filter_by(id=1)
        session2.commit()
        session2.close()

        expected = (14, 201)
        assert expected == res[0].ptest

    def test_point_type_insert_select_nonetype(self, SessionFactory, PointTable):
        """
        Checks that none-type point type data is inserted and retrieved correctly.
        """

        session = SessionFactory()
        session.add(PointTable(id=2, ptest=None))
        session.commit()
        session.close()

        session2 = SessionFactory()
        res = session2.query(PointTable).filter_by(id=2)
        session2.commit()
        session2.close()

        expected = None
        assert expected == res[0].ptest

    def test_point_type_insert_select_none_in_tuple_type(self, SessionFactory, PointTable):
        """
        Checks that none-type point type data is inserted and retrieved correctly.
        """

        session = SessionFactory()
        session.add(PointTable(id=3, ptest=(None, None)))
        session.commit()
        session.close()

        session2 = SessionFactory()
        res = session2.query(PointTable).filter_by(id=3)
        session2.commit()
        session2.close()

        expected = None
        assert expected == res[0].ptest

    def test_point_type_insert_select_mixed_type(self, SessionFactory, PointTable):
        """
        Checks that exception is raised when mixed none and non-none-type data is passed
        """
        session = SessionFactory()
        with pytest.raises(StatementError):
            try:
                session.add(PointTable(id=4, ptest=(1, None)))
                session.commit()
                session.close()
            except Exception as e:
                session.close()
                raise e

    def test_point_type_insert_select_mixed_type_2(self, SessionFactory, PointTable):
        """
        Checks that none-type point type data is inserted and retrieved correctly.
        """
        session = SessionFactory()
        with pytest.raises(StatementError):
            try:
                session.add(PointTable(id=5, ptest=(None, 1)))
                session.commit()
                session.close()
            except Exception as e:
                session.close()
                raise e
