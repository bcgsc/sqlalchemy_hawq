from unittest import mock


import pytest
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Text, UniqueConstraint, create_engine
from sqlalchemy.schema import CreateTable, Index
from hawq_sqlalchemy.partition import *

@pytest.fixture
def engine_spy():
    class MetadataDumpSpy:
        def __init__(self):
            self.sql = None
            self.engine = None

        def __call__(self, sql, *args, **kwargs):
            self.sql = str(sql.compile(dialect=self.engine.dialect))
    spy = MetadataDumpSpy()
    engine = create_engine('hawq://localhost/creisle', strategy='mock', executor=spy)
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
\tchrom TEXT NOT NULL
)
WITH (appendonly=True)
DISTRIBUTED BY (chrom)
PARTITION BY LIST (chrom)
(
\tPARTITION chr1 VALUES ('1'),
\tPARTITION chr2 VALUES ('2'),
\tPARTITION chr3 VALUES ('3'),
\tDEFAULT PARTITION other
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
\tchrom TEXT NOT NULL
)
PARTITION BY LIST (chrom)
(
\tPARTITION chr1 VALUES ('1'),
\tPARTITION chr2 VALUES ('2'),
\tPARTITION chr3 VALUES ('3'),
\tDEFAULT PARTITION other
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
\tchrom INTEGER NOT NULL
)
PARTITION BY RANGE (chrom)

(
\tSTART (0) END (10) EVERY (2),
\tDEFAULT PARTITION extra
)'''
        assert expected == engine_spy.sql.strip()

    def test_subpartition_by_list_and_range(self, base, engine_spy):

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
        actual = engine_spy.sql.strip()
        print(actual)
        expected = '''CREATE TABLE "MockTable" (
	id INTEGER NOT NULL, 
	year INTEGER, 
	month INTEGER, 
	chrom TEXT
)
PARTITION BY RANGE (year)
\n\tSUBPARTITION BY RANGE (month)
\tSUBPARTITION TEMPLATE
\t(
\t\tSTART (1) END (13) EVERY (1),
\t\tDEFAULT SUBPARTITION extra
\t)

\tSUBPARTITION BY LIST (chrom)\
\n\tSUBPARTITION TEMPLATE
\t(
\t\tSUBPARTITION chr1 VALUES ('1'),
\t\tSUBPARTITION chr2 VALUES ('2'),
\t\tSUBPARTITION chr3 VALUES ('3'),
\t\tDEFAULT SUBPARTITION other
\t)
(
\tSTART (2002) END (2012) EVERY (1),
\tDEFAULT PARTITION extra\
\n)'''
        assert expected == actual


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
