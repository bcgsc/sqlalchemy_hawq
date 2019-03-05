"""
Defines pytest tests the require a live db connection.
At the experimental stage.
"""

import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import configure_mappers
from sqlalchemy.orm import relationship


from hawq_sqlalchemy.point import Point


@pytest.fixture
def base():
    return declarative_base()


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
            __table_args__ = {'schema':'testschema'}

        base.metadata.create_all(test_engine)

        conn = test_engine.connect()

        ins = MockTable.__table__.insert(inline=True).values(test=5)

        conn.execute(ins)
        Session = sessionmaker(bind=test_engine)
        session = Session()
        x = session.query(MockTable).all()

        print(test_engine.dialect)
        assert x[0].test == 5


    def test_point_type_insert_select(self, base, test_engine):
        """
        Checks that point type data can be inserted and selected.
        """
        class MockTable2(base):
            __tablename__ = 'mocktable2'
            id = Column('id', Integer, primary_key=True)
            ptest = Column('ptest', Point)
            __table_args__ = {'schema':'testschema'}
        base.metadata.create_all(test_engine)

        conn = test_engine.connect()
        ins = MockTable2.__table__.insert(inline=True).values(id=10, ptest={'x':14, 'y':201})
        print(ins.compile().params)
        conn.execute(ins)

        Session = sessionmaker(bind=test_engine)
        session = Session()
        x = session.query(MockTable2).all()

        expected = (14, 201)
        assert expected == x[0].ptest
