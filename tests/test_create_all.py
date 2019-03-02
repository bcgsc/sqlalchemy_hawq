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


@pytest.fixture
def engine_spy():
    class MetadataDumpSpy:
        def __init__(self):
            self.sql = None
            self.engine = None

        def __call__(self, sql, *args, **kwargs):
            self.sql = str(sql.compile(dialect=self.engine.dialect))
    spy = MetadataDumpSpy()
    engine = create_engine('hawq://localhost/elewis', strategy='mock', executor=spy)
    spy.engine = engine
    return spy


@pytest.fixture
def base():
    return declarative_base()


class TestCreateAll:
    def test_multiple(self, base, engine_spy):

        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {
                    'hawq_distributed_by': 'chrom',
                    'hawq_partition_by': ListPartition('chrom', {'chr1': '1', 'chr2': '2', 'chr3': '3'}),
                    'hawq_appendonly': True
                }
            )
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = '''CREATE TABLE "MockTable" (
	chrom TEXT NOT NULL
)
WITH (appendonly=True)
DISTRIBUTED BY (chrom)
PARTITION BY LIST (chrom)
(
    PARTITION chr1 VALUES ('1'),
    PARTITION chr2 VALUES ('2'),
    PARTITION chr3 VALUES ('3'),
    DEFAULT PARTITION other
)'''
        assert expected == engine_spy.sql.strip()

    def test_distributed_by(self, base, engine_spy):

        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {
                    'hawq_distributed_by': 'chrom'
                }
            )
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = '''CREATE TABLE "MockTable" (
\tchrom TEXT NOT NULL
)
DISTRIBUTED BY (chrom)'''
        assert expected == engine_spy.sql.strip()

    def test_partition_by_list(self, base, engine_spy):

        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {
                    'hawq_partition_by': ListPartition('chrom', {'chr1': '1', 'chr2': '2', 'chr3': '3'})
                }
            )
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = '''CREATE TABLE "MockTable" (
	chrom TEXT NOT NULL
)
PARTITION BY LIST (chrom)
(
    PARTITION chr1 VALUES ('1'),
    PARTITION chr2 VALUES ('2'),
    PARTITION chr3 VALUES ('3'),
    DEFAULT PARTITION other
)'''
        assert expected == engine_spy.sql.strip()

    def test_partition_by_range(self, base, engine_spy):

        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {
                    'hawq_partition_by': RangePartition('chrom', 0, 10, 2)
                }
            )
            chrom = Column('chrom', Integer(), primary_key=True, autoincrement=False)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = '''CREATE TABLE "MockTable" (
	chrom INTEGER NOT NULL
)
PARTITION BY RANGE (chrom)
(
    START (0) END (10) EVERY (2),
    DEFAULT PARTITION extra
)'''
        assert expected == engine_spy.sql.strip()
        print(engine_spy.sql.strip())

    def test_partition_by_range_subpartition_by_list_and_range(self, base, engine_spy):

        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                {
                    'hawq_partition_by': RangePartition('year', 2002, 2012, 1, [
                        RangeSubpartition('month', 1, 13, 1),
                        ListSubpartition('chrom', {'chr1': '1', 'chr2':'2', 'chr3':'3'})
                    ])
                }
            )
            id = Column('id',Integer(), primary_key = True, autoincrement = False)
            year = Column('year',Integer())
            month = Column('month',Integer())
            chrom = Column('chrom',Text())
        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = '''CREATE TABLE "MockTable" (
	id INTEGER NOT NULL, 
	year INTEGER, 
	month INTEGER, 
	chrom TEXT
)
PARTITION BY RANGE (year)
    SUBPARTITION BY RANGE (month)
    SUBPARTITION TEMPLATE
    (
        START (1) END (13) EVERY (1),
        DEFAULT SUBPARTITION extra
    )

    SUBPARTITION BY LIST (chrom)
    SUBPARTITION TEMPLATE
    (
        SUBPARTITION chr1 VALUES ('1'),
        SUBPARTITION chr2 VALUES ('2'),
        SUBPARTITION chr3 VALUES ('3'),
        DEFAULT SUBPARTITION other
    )
(
    START (2002) END (2012) EVERY (1),
    DEFAULT PARTITION extra
)'''
        assert expected == engine_spy.sql.strip()


    def test_partition_by_list_subpartition_by_range_and_range(self, base, engine_spy):

        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                {
                    'hawq_partition_by': ListPartition('chrom', {'chr1': '1', 'chr2':'2', 'chr3':'3'}, [
                        RangeSubpartition('year', 2002, 2012, 1),
                        RangeSubpartition('month', 1, 13, 1),
                    ])
                }
            )
            id = Column('id',Integer(), primary_key = True, autoincrement = False)
            year = Column('year',Integer())
            month = Column('month',Integer())
            chrom = Column('chrom',Text())
        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)

        expected = '''CREATE TABLE "MockTable" (
	id INTEGER NOT NULL, 
	year INTEGER, 
	month INTEGER, 
	chrom TEXT
)
PARTITION BY LIST (chrom)
    SUBPARTITION BY RANGE (year)
    SUBPARTITION TEMPLATE
    (
        START (2002) END (2012) EVERY (1),
        DEFAULT SUBPARTITION extra
    )

    SUBPARTITION BY RANGE (month)
    SUBPARTITION TEMPLATE
    (
        START (1) END (13) EVERY (1),
        DEFAULT SUBPARTITION extra
    )
(
    PARTITION chr1 VALUES ('1'),
    PARTITION chr2 VALUES ('2'),
    PARTITION chr3 VALUES ('3'),
    DEFAULT PARTITION other
)'''
        assert expected == engine_spy.sql.strip()


    def test_appendonly(self, base, engine_spy):

        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {
                    'hawq_appendonly': True,

                }
            )
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = '''CREATE TABLE "MockTable" (
\tchrom TEXT NOT NULL
)
WITH (appendonly=True)'''
        assert expected == engine_spy.sql.strip()

    def test_appendonly_error(self, base, engine_spy):

        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {
                    'hawq_appendonly': 'bad value',

                }
            )
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata

        with pytest.raises(ValueError):
            metadata.create_all(engine_spy.engine)

    def test_orientation(self, base, engine_spy):

        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {
                    'hawq_orientation': 'row',

                }
            )
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = '''CREATE TABLE "MockTable" (
\tchrom TEXT NOT NULL
)
WITH (orientation=ROW)'''
        assert expected == engine_spy.sql.strip()

    def test_orientation_error(self, base, engine_spy):

        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {
                    'hawq_orientation': 'bad value',

                }
            )
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata

        with pytest.raises(ValueError):
            metadata.create_all(engine_spy.engine)

    def test_compresstype(self, engine_spy):

        for compresstype in {'ZLIB', 'SNAPPY', 'GZIP', 'NONE'}:
            Base = declarative_base()
            class MockTable(Base):
                __tablename__ = 'MockTable'
                __table_args__ = (
                    UniqueConstraint('chrom'),
                    {
                        'hawq_compresstype': compresstype,

                    }
                )
                chrom = Column('chrom', Text(), primary_key=True)

            metadata = MockTable.__table__.metadata
            metadata.create_all(engine_spy.engine)
            expected = '''CREATE TABLE "MockTable" (
\tchrom TEXT NOT NULL
)
WITH (compresstype={})'''.format(compresstype)
            assert expected == engine_spy.sql.strip()

    def test_compresstype_error(self, base, engine_spy):

        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {
                    'hawq_compresstype': 'tar',

                }
            )
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata

        with pytest.raises(ValueError):
            metadata.create_all(engine_spy.engine)

    def test_compresslevel(self, engine_spy):

        for compresslevel in range(10):
            Base = declarative_base()
            class MockTable(Base):
                __tablename__ = 'MockTable'
                __table_args__ = (
                    UniqueConstraint('chrom'),
                    {
                        'hawq_compresslevel': compresslevel,

                    }
                )
                chrom = Column('chrom', Text(), primary_key=True)

            metadata = MockTable.__table__.metadata
            metadata.create_all(engine_spy.engine)
            expected = '''CREATE TABLE "MockTable" (
\tchrom TEXT NOT NULL
)
WITH (compresslevel={})'''.format(compresslevel)
            assert expected == engine_spy.sql.strip()

    def test_compresslevel_error(self, base, engine_spy):

        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {
                    'hawq_compresslevel': 10,

                }
            )
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata

        with pytest.raises(ValueError):
            metadata.create_all(engine_spy.engine)


    def test_point_type(self, base, engine_spy):
        class MockTable(base):
            __tablename__ = 'MockTable'
            ptest = Column('ptest', Point, primary_key=True)


        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = '''CREATE TABLE "MockTable" (
\tptest POINT NOT NULL
)'''
        assert expected == engine_spy.sql.strip()

    def test_compile_point_type_from_list_input(self, base, engine_spy):

        
        class MockTable(base):
            __tablename__ = 'MockTable'

            id = Column('id', Integer, primary_key=True)
            ptest = Column('ptest', Point)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)

        ins = MockTable.__table__.insert().values(id=3, ptest=[3,4])
        params = ins.compile().params
        expected =  {'id': 3, 'ptest': 'POINT(3,4)'}

        assert expected == params