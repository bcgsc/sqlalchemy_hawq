from unittest import mock


import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Text, UniqueConstraint, create_engine, table, column, Table, MetaData, ForeignKey
from sqlalchemy.schema import CreateTable, Index
from sqlalchemy import func, select, insert
from hawq_sqlalchemy.partition import RangePartition, ListPartition, RangeSubpartition, ListSubpartition
from hawq_sqlalchemy.ddl import Point
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import dml
from sqlalchemy.orm import configure_mappers
import hawq_sqlalchemy
from unittest.mock import MagicMock
from sqlalchemy.testing.mock import call
from sqlalchemy.testing.mock import Mock
from sqlalchemy.testing.mock import patch
from sqlalchemy.schema import CreateTable
from sqlalchemy.orm import configure_mappers


@pytest.fixture
def base():
    return declarative_base()


class TestWithLiveConnection:

    def test_live_setup(self, base, test_engine):

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


    def test_retrieve_point_type(self, base, test_engine):
        class MockTable2(base):
            __tablename__ = 'mocktable2'
            id = Column('id', Integer, primary_key=True)
            ptest = Column('ptest', Point)
            __table_args__ = {'schema':'testschema'}
        base.metadata.create_all(test_engine)

        conn = test_engine.connect()
        ins = MockTable2.__table__.insert(inline=True).values(id=10, ptest=[11,12])
        print(ins.compile().params)
        conn.execute(ins)

        Session = sessionmaker(bind=test_engine)
        session = Session()
        x = session.query(MockTable2).all()

        expected = [11.0,12.0]
        assert expected == x[0].ptest
        
