"""
Tests Hawq compiler output without connecting to live db.
"""
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, UniqueConstraint, create_engine, Text
from sqlalchemy.testing.suite import fixtures
from sqlalchemy.testing import assert_raises
from collections import OrderedDict
import re

from sqlalchemy_hawq.partition import (
    RangePartition,
    ListPartition,
    RangeSubpartition,
    ListSubpartition,
)
from sqlalchemy_hawq.point import Point


def get_engine_spy():
    class MetadataDumpSpy:
        def __init__(self):
            self.sql = None
            self.engine = None

        def __call__(self, sql, *args, **kwargs):
            self.sql = str(sql.compile(dialect=self.engine.dialect))

    spy = MetadataDumpSpy()
    engine = create_engine('hawq://localhost/dummy_user', strategy='mock', executor=spy)
    spy.engine = engine
    return spy


def normalize_whitespace(input_string):
    '''
    Strip whitespaces and newline characters from the input string

    Args:
        input_string (str): Given input string

    Returns:
        str: String sans whitespaces and newline characters
    '''

    # Use regex to strip whitespaces and newline characters
    return re.sub(r'[\n\s]+', ' ', input_string, flags=re.MULTILINE)


class TestCreateAll(fixtures.TestBase):
    def test_multiple(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {
                    'hawq_distributed_by': 'chrom',
                    'hawq_partition_by': ListPartition(
                        'chrom', OrderedDict([('chr1', '1'), ('chr2', '2'), ('chr3', '3')])
                    ),
                    'hawq_appendonly': True,
                },
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

        normalize_whitespace(expected) == normalize_whitespace(engine_spy.sql)

    def test_distributed_by(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (UniqueConstraint('chrom'), {'hawq_distributed_by': 'chrom'})
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = '''CREATE TABLE "MockTable" (
chrom TEXT NOT NULL
)
DISTRIBUTED BY (chrom)'''

        normalize_whitespace(expected) == normalize_whitespace(engine_spy.sql)

    def test_distributed_with_hash(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {'hawq_distributed_by': 'chrom', 'hawq_bucketnum': 42},
            )
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = '''CREATE TABLE "MockTable" (
chrom TEXT NOT NULL
)
WITH (bucketnum=42)
DISTRIBUTED BY (chrom)'''

        normalize_whitespace(expected) == normalize_whitespace(engine_spy.sql)

    def test_hash_without_distribution(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (UniqueConstraint('chrom'), {'hawq_bucketnum': 42})
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        assert_raises(ValueError, metadata.create_all, engine_spy.engine)

    def test_partition_by_list(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {
                    'hawq_partition_by': ListPartition(
                        'chrom', OrderedDict([('chr1', '1'), ('chr2', '2'), ('chr3', '3')])
                    )
                },
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
        normalize_whitespace(expected) == normalize_whitespace(engine_spy.sql)

    def test_partition_by_range(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (
                UniqueConstraint('chrom'),
                {'hawq_partition_by': RangePartition('chrom', 0, 10, 2)},
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
        normalize_whitespace(expected) == normalize_whitespace(engine_spy.sql)

    def test_partition_by_range_subpartition_by_list_and_range(
        self, base=declarative_base(), engine_spy=get_engine_spy()
    ):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = {
                'hawq_partition_by': RangePartition(
                    'year',
                    2002,
                    2012,
                    1,
                    [
                        RangeSubpartition('month', 1, 13, 1),
                        ListSubpartition(
                            'chrom', OrderedDict([('chr1', '1'), ('chr2', '2'), ('chr3', '3')])
                        ),
                    ],
                )
            }
            id = Column('id', Integer(), primary_key=True, autoincrement=False)
            year = Column('year', Integer())
            month = Column('month', Integer())
            chrom = Column('chrom', Text())

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = ' '.join(
            '''CREATE TABLE "MockTable" (
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
)'''.split()
        )
        assert expected == ' '.join((engine_spy.sql.strip()).split())

    def test_partition_by_list_subpartition_by_range_and_range(
        self, base=declarative_base(), engine_spy=get_engine_spy()
    ):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = {
                'hawq_partition_by': ListPartition(
                    'chrom',
                    OrderedDict([('chr1', '1'), ('chr2', '2'), ('chr3', '3')]),
                    [
                        RangeSubpartition('year', 2002, 2012, 1),
                        RangeSubpartition('month', 1, 13, 1),
                    ],
                )
            }
            id = Column('id', Integer(), primary_key=True, autoincrement=False)
            year = Column('year', Integer())
            month = Column('month', Integer())
            chrom = Column('chrom', Text())

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = ' '.join(
            '''CREATE TABLE "MockTable" (
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
)'''.split()
        )
        assert expected == ' '.join((engine_spy.sql.strip()).split())

    def test_appendonly(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (UniqueConstraint('chrom'), {'hawq_appendonly': True})
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = '''CREATE TABLE "MockTable" (
chrom TEXT NOT NULL
)
WITH (appendonly=True)'''

        normalize_whitespace(expected) == normalize_whitespace(engine_spy.sql)

    def test_appendonly_error(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (UniqueConstraint('chrom'), {'hawq_appendonly': 'bad value'})
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        assert_raises(ValueError, metadata.create_all, engine_spy.engine)

    def test_orientation(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (UniqueConstraint('chrom'), {'hawq_orientation': 'row'})
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = '''CREATE TABLE "MockTable" (
chrom TEXT NOT NULL
)
WITH (orientation=ROW)'''

        normalize_whitespace(expected) == normalize_whitespace(engine_spy.sql)

    def test_orientation_error(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (UniqueConstraint('chrom'), {'hawq_orientation': 'bad value'})
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        assert_raises(ValueError, metadata.create_all, engine_spy.engine)

    def test_compresstype(self, engine_spy=get_engine_spy()):

        for compresstype in {'ZLIB', 'SNAPPY', 'GZIP', 'NONE'}:
            Base = declarative_base()

            class MockTable(Base):
                __tablename__ = 'MockTable'
                __table_args__ = (UniqueConstraint('chrom'), {'hawq_compresstype': compresstype})
                chrom = Column('chrom', Text(), primary_key=True)

            metadata = MockTable.__table__.metadata
            metadata.create_all(engine_spy.engine)
            expected = '''CREATE TABLE "MockTable" (
chrom TEXT NOT NULL
)
WITH (compresstype={})'''.format(
                compresstype
            )

        normalize_whitespace(expected) == normalize_whitespace(engine_spy.sql)

    def test_compresstype_error(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (UniqueConstraint('chrom'), {'hawq_compresstype': 'tar'})
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        assert_raises(ValueError, metadata.create_all, engine_spy.engine)

    def test_compresslevel(self, engine_spy=get_engine_spy()):

        for compresslevel in range(10):
            Base = declarative_base()

            class MockTable(Base):
                __tablename__ = 'MockTable'
                __table_args__ = (UniqueConstraint('chrom'), {'hawq_compresslevel': compresslevel})
                chrom = Column('chrom', Text(), primary_key=True)

            metadata = MockTable.__table__.metadata
            metadata.create_all(engine_spy.engine)
            expected = '''CREATE TABLE "MockTable" (
chrom TEXT NOT NULL
)
WITH (compresslevel={})'''.format(
                compresslevel
            )

        normalize_whitespace(expected) == normalize_whitespace(engine_spy.sql)

    def test_compresslevel_error(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'
            __table_args__ = (UniqueConstraint('chrom'), {'hawq_compresslevel': 10})
            chrom = Column('chrom', Text(), primary_key=True)

        metadata = MockTable.__table__.metadata
        assert_raises(ValueError, metadata.create_all, engine_spy.engine)

    def test_point_type(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'
            ptest = Column('ptest', Point, primary_key=True)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)
        expected = '''CREATE TABLE "MockTable" (
ptest POINT NOT NULL
)'''
        normalize_whitespace(expected) == normalize_whitespace(engine_spy.sql)

    def test_compile_point_type_from_list_input(
        self, base=declarative_base(), engine_spy=get_engine_spy()
    ):
        class MockTable(base):
            __tablename__ = 'MockTable'

            id = Column('id', Integer, primary_key=True)
            ptest = Column('ptest', Point)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)

        ins = MockTable.__table__.insert().values(id=3, ptest=(3, 4))
        params = ins.compile().params
        expected = {'id': 3, 'ptest': (3, 4)}

        assert expected == params

    def test_delete_statement_with_filter_clauses(
        self, base=declarative_base(), engine_spy=get_engine_spy()
    ):
        class MockTable(base):
            __tablename__ = 'MockTable'

            id = Column('id', Integer, primary_key=True)
            ptest = Column('ptest', Point)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)

        delete_stmt = MockTable.__table__.delete().where(id == 3)
        assert_raises(NotImplementedError, delete_stmt.compile, engine_spy.engine)

    def test_delete_statement_bare(self, base=declarative_base(), engine_spy=get_engine_spy()):
        class MockTable(base):
            __tablename__ = 'MockTable'

            id = Column('id', Integer, primary_key=True)
            ptest = Column('ptest', Point)

        metadata = MockTable.__table__.metadata
        metadata.create_all(engine_spy.engine)

        delete_stmt = MockTable.__table__.delete()
        expected = str(delete_stmt.compile(engine_spy.engine))

        assert expected == 'TRUNCATE TABLE \"MockTable\"'
