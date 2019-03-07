"""
Defines pytest tests the require a live db connection.
At the experimental stage.
"""

import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import StatementError


from hawq_sqlalchemy.point import Point
from hawq_sqlalchemy.point import SQLAlchemyHawqException


@pytest.fixture(scope='class')
def base():
    return declarative_base()


@pytest.fixture(scope='class')
def PointTable(base, test_engine):
    class PointTable(base):
        __tablename__ = 'point_table'
        id = Column('id', Integer, primary_key=True)
        ptest = Column('ptest', Point)
        __table_args__ = {'schema': 'testschema'}
    base.metadata.create_all(test_engine)
    return PointTable


class TestWithLiveConnection:
    """
    A group of tests requiring a live connection.
    """
    def test_live_setup(self, base, test_engine):
        """
        Checks that the live connection works, ie
        that a table can be made and data entered and queried.
        """
        class MockTable(base):
            __tablename__ = 'mocktable'
            id = Column('id', Integer, primary_key=True)
            test = Column('test', Integer)
            __table_args__ = {'schema': 'testschema'}

        base.metadata.create_all(test_engine)

        conn = test_engine.connect()

        ins = MockTable.__table__.insert(inline=True).values(test=5)

        conn.execute(ins)
        Session = sessionmaker(bind=test_engine)
        session = Session()
        res = session.query(MockTable).all()
        session.close()

        assert res[0].test == 5


    def test_point_type_insert_select(self, test_engine, PointTable):
        """
        Checks that point type data can be inserted and selected.
        """
        conn = test_engine.connect()
        ins = PointTable.__table__.insert(inline=True).values(id=1, ptest=(14, 201))
        conn.execute(ins)

        Session = sessionmaker(bind=test_engine)
        session = Session()
        res = session.query(PointTable).filter_by(id=1)
        session.close()

        expected = (14, 201)
        assert expected == res[0].ptest


    def test_point_type_insert_select_nonetype(self, test_engine, PointTable):
        """
        Checks that none-type point type data is inserted and retrieved correctly.
        """
        conn = test_engine.connect()
        ins = PointTable.__table__.insert(inline=True).values(id=2, ptest=None)
        conn.execute(ins)

        Session = sessionmaker(bind=test_engine)
        session = Session()
        res = session.query(PointTable).filter_by(id=2)
        session.close()

        expected = None
        assert expected == res[0].ptest

    def test_point_type_insert_select_none_in_tuple_type(self, test_engine, PointTable):
        """
        Checks that none-type point type data is inserted and retrieved correctly.
        """
        conn = test_engine.connect()
        ins = PointTable.__table__.insert(inline=True).values(id=3, ptest=(None,None))
        conn.execute(ins)

        Session = sessionmaker(bind=test_engine)
        session = Session()
        res = session.query(PointTable).filter_by(id=3)
        session.close()

        expected = None
        assert expected == res[0].ptest

    def test_point_type_insert_select_mixed_type(self, test_engine, PointTable):
        """
        Checks that exception is raised when mixed none and non-none-type data is passed
        """
        conn = test_engine.connect()
        ins = PointTable.__table__.insert(inline=True).values(id=4, ptest=(1,None))
        with pytest.raises(StatementError):
            conn.execute(ins)

    def test_point_type_insert_select_mixed_type_2(self, test_engine, PointTable):
        """
        Checks that none-type point type data is inserted and retrieved correctly.
        """
        conn = test_engine.connect()
        ins = PointTable.__table__.insert(inline=True).values(id=5, ptest=(None,1))
        with pytest.raises(StatementError):
            conn.execute(ins)