# Sqlalchemy Hawq

[![Build Status](https://travis-ci.org/bcgsc/sqlalchemy_hawq.svg?branch=master)](https://travis-ci.org/bcgsc/sqlalchemy_hawq)

- [Getting Started](#getting-started)
  - [Install (For developers)](#install-for-developers)
  - [Run Tests](#run-tests)
  - [Deploy (For developers)](#deploy-for-developers)
- [Using in a SQLAlchemy project](#using-in-a-sqlalchemy-project)
  - [How to incorporate sqlalchemy-hawq](#how-to-incorporate-sqlalchemy-hawq)
  - [Hawq-specific table arguments](#hawq-specific-table-arguments)
  - [Example of hawq table arguments with declarative syntax](#example-of-hawq-table-arguments-with-declarative-syntax)
- [Using partitions](#using-partitions)

This is a custom dialect for using SQLAlchemy with a [HAWQ](http://hawq.apache.org/docs/userguide/2.3.0.0-incubating/tutorial/overview.html)
database.

It extends the Postgresql dialect.

Features include:
- Hawq options for 'CREATE TABLE' statements
- a point class
- a modified 'DELETE' statement for compatibility with SQLAlchemy's test suite

Unless specifically overridden, any functionality in SQLAlchemy's Postgresql dialect is also available. Note that in general, functionality that is available in Postgresql but not in Hawq has not yet been disabled.


## Getting Started

### Install (For developers)


clone this repository

```bash
git clone https://creisle@svn.bcgsc.ca/bitbucket/scm/dat/sqlalchemy_hawq.git
cd sqlalchemy_hawq
```

create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

install the package and its development dependencies

```bash
pip install -e .[dev]
```

### Run Tests

sqlalchemy_hawq incorporates the standard SQLAlchemy test suite as well as some tests of its own. Run them all as follows:

```bash
pytest test --hawq://username:password@hostname:port/database
```

Run only the standard SQLAlchemy test suite:

```bash
pytest test --hawq://username:password@hostname:port/database --sqla-only
```

Run only the custom sqlalchemy_hawq tests:

```bash
pytest test --hawq://username:password@hostname:port/database --custom-only
```

Run only the custom tests that don't require a live db connection - e.g., for ci:

```bash
pytest test --offline-only
```

For tests that use a live db connection, user running the tests must be able to create and drop tables on the db provided. Also, many of the tests require that there are pre-existing schemas 'test_schema' and 'test_schema_2' on the db. The test suite can be run without them but the tests will fail.

See https://github.com/zzzeek/sqlalchemy/blob/master/README.unittests.rst and https://github.com/zzzeek/sqlalchemy/blob/master/README.dialects.rst for more information on test configuration. Note that no default db url is stored in sqlalchemy_hawq's setup.cfg.

### Deploy (For developers)

Create the venv and ensure the latest versions of setuptools and pip are installed:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -U setuptools pip
```

Install sqlalchemy_hawq for deployment and create the distribution packages:

```bash
pip install .[deploy]
python setup.py install sdist bdist_wheel
```

If you want, you can now check for any problems in the distribution files:

```bash
twine check dist/*
```

Then:

```bash
twine upload dist/* --repository-url http://pyshop.bcgsc.ca/simple/
```

---

## Using in a SQLAlchemy project

### How to incorporate sqlalchemy-hawq

Add sqlalchemy_hawq to your dependencies and install.

```bash
pip install sqlalchemy_hawq
```

Then the plugin can be used like any other engine

```python
from sqlalchemy import create_engine

engine = create_engine('hawq://USERNAME:PASSWORD@hdp-master02.hadoop.bcgsc.ca:5432/test_refactor/')
```

For sqlalchemy's instructions on how to use their engine, see https://docs.sqlalchemy.org/en/13/core/engines.html.


### Hawq-specific table arguments

Hawq specific table arguments are also supported (Not all features are supported yet)

| Argument            | Type                            | Example                                                                                                                                            | Notes                                                               |
| ------------------- | ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| hawq_distributed_by | str                             | `'column_name'`                                                                                                                                    |                                                                     |
| hawq_partition_by   | RangePartition or ListPartition | `ListPartition('chrom', {'chr1': '1', 'chr2':'2', 'chr3':'3'}, [RangeSubpartition('year', 2002, 2012, 1), RangeSubpartition('month', 1, 13, 1),])` | Does not currently support range partitioning on dates              |
| hawq_apppendonly    | bool                            | `True`                                                                                                                                             |                                                                     |
| hawq_orientation    | str                             | `'ROW'`                                                                                                                                            | expects one of `{'ROW', 'PARQUET'}`                                 |
| hawq_compresstype   | str                             | `'ZLIB'`                                                                                                                                           | expects one of `{'ZLIB', 'SNAPPY', 'GZIP', 'NONE'}`                 |
| hawq_compresslevel  | int                             | `0`                                                                                                                                                | expects an integer between 0-9                                      |
| hawq_bucketnum      | int                             | `6`                                                                                                                                                | expects an integer between 0 and `default_hash_table_bucket_number` |


### Example of hawq table arguments with declarative syntax

```python
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Text

Base = declarative_base()

class ExampleTable(Base):
    __tablename__ = 'example_table'

    __table_args__ = {
        'hawq_distributed_by': 'attr1'
        'hawq_appendonly': 'True'
    }

    attr1 = Column(Integer())
    attr2 = Column(Integer())


def main():
    engine = create_engine('hawq://USERNAME:PASSWORD@hdp-master02.hadoop.bcgsc.ca:5432/test_refactor/')
    engine.create_all()
```

---

## Using partitions

See https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/ddl/ddl-partition.html for an extended discussion of how partitions work in Hawq.

Basically, partitioning divides a table into several smaller tables on the value of one or more columns, in order to reduce search time on those columns. The parent table can then be queried/added to without any further reference to the partitions, as Hawq handles all the parent-partition interactions.

Partition arguments are:

```python
RangePartition(
    column_name=str,
    start=int,
    end=int,
    every=int,
    subpartitions=[])
```
or

```python
ListPartition(
    column_name=str,
    columns=dict{name_of_partition:value_to_partition_on},
    subpartitions=[])
```

where 'subpartitions' is an array of RangeSubpartitions and/or ListSubpartitions.

Subpartition arguments are


```python
RangeSubpartition(
    column_name=str,
    start=int,
    end=int,
    every=int)
```
or

```python
ListSubpartition(
    column_name=str,
    columns=dict{name_of_partition:value_to_partition_on})
```

Note that the params are the same for the Subpartitions are for the Partitions, except that Subpartitions do not have a nested subpartition array.

Partition level is determined by the order of the subpartitions in the subpartition array.


Using sqlalchemy-hawq syntax to define a partition:

```python
class MockTable(base):
    __tablename__ = 'MockTable'
    __table_args__ = {
        'hawq_partition_by': RangePartition(
            'year',
            2009,
            2012,
            1,
            [
                RangeSubpartition(
                    'quarter',
                    1,
                    5,
                    1),
                ListSubpartition(
                    'chrom',
                    {
                        'chr1': '1',
                        'chr2': '2',
                        'chr3': '3'}),
            ],
        )
    }
    id = Column('id', Integer(), primary_key=True, autoincrement=False)
    year = Column('year', Integer())
    quarter = Column('quarter', Integer())
    chrom = Column('chrom', Text())
```

The SQL output:

```sql
'''CREATE TABLE "MockTable" (
	id INTEGER NOT NULL,
	year INTEGER,
	quarter INTEGER,
	chrom TEXT
)
PARTITION BY RANGE (year)
    SUBPARTITION BY RANGE (quarter)
    SUBPARTITION TEMPLATE
    (
        START (1) END (5) EVERY (1),
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
    START (2009) END (2012) EVERY (2),
    DEFAULT PARTITION extra
)'''
```


The resulting tables:


```sql
test_refactor=> \dt
                            List of relations
 Schema |                     Name                      | Type  |  Owner
--------+-----------------------------------------------+-------+---------
 public | MockTable                                     | table | elewis
 public | MockTable_1_prt_2                             | table | elewis
 public | MockTable_1_prt_2_2_prt_2                     | table | elewis
 public | MockTable_1_prt_2_2_prt_2_3_prt_chr1          | table | elewis
 public | MockTable_1_prt_2_2_prt_2_3_prt_chr2          | table | elewis
 public | MockTable_1_prt_2_2_prt_2_3_prt_chr3          | table | elewis
 public | MockTable_1_prt_2_2_prt_2_3_prt_other         | table | elewis
 public | MockTable_1_prt_2_2_prt_3                     | table | elewis
 public | MockTable_1_prt_2_2_prt_3_3_prt_chr1          | table | elewis
 public | MockTable_1_prt_2_2_prt_3_3_prt_chr2          | table | elewis
 public | MockTable_1_prt_2_2_prt_3_3_prt_chr3          | table | elewis
 public | MockTable_1_prt_2_2_prt_3_3_prt_other         | table | elewis
 public | MockTable_1_prt_2_2_prt_4                     | table | elewis
 public | MockTable_1_prt_2_2_prt_4_3_prt_chr1          | table | elewis
 public | MockTable_1_prt_2_2_prt_4_3_prt_chr2          | table | elewis
 public | MockTable_1_prt_2_2_prt_4_3_prt_chr3          | table | elewis
 public | MockTable_1_prt_2_2_prt_4_3_prt_other         | table | elewis
 public | MockTable_1_prt_2_2_prt_5                     | table | elewis
 public | MockTable_1_prt_2_2_prt_5_3_prt_chr1          | table | elewis
 public | MockTable_1_prt_2_2_prt_5_3_prt_chr2          | table | elewis
 public | MockTable_1_prt_2_2_prt_5_3_prt_chr3          | table | elewis
 public | MockTable_1_prt_2_2_prt_5_3_prt_other         | table | elewis
 public | MockTable_1_prt_2_2_prt_extra                 | table | elewis
 public | MockTable_1_prt_2_2_prt_extra_3_prt_chr1      | table | elewis
 public | MockTable_1_prt_2_2_prt_extra_3_prt_chr2      | table | elewis
 public | MockTable_1_prt_2_2_prt_extra_3_prt_chr3      | table | elewis
 public | MockTable_1_prt_2_2_prt_extra_3_prt_other     | table | elewis
 public | MockTable_1_prt_3                             | table | elewis
 public | MockTable_1_prt_3_2_prt_2                     | table | elewis
 public | MockTable_1_prt_3_2_prt_2_3_prt_chr1          | table | elewis
 public | MockTable_1_prt_3_2_prt_2_3_prt_chr2          | table | elewis
 public | MockTable_1_prt_3_2_prt_2_3_prt_chr3          | table | elewis
 public | MockTable_1_prt_3_2_prt_2_3_prt_other         | table | elewis
 public | MockTable_1_prt_3_2_prt_3                     | table | elewis
 public | MockTable_1_prt_3_2_prt_3_3_prt_chr1          | table | elewis
 public | MockTable_1_prt_3_2_prt_3_3_prt_chr2          | table | elewis
 public | MockTable_1_prt_3_2_prt_3_3_prt_chr3          | table | elewis
 public | MockTable_1_prt_3_2_prt_3_3_prt_other         | table | elewis
 public | MockTable_1_prt_3_2_prt_4                     | table | elewis
 public | MockTable_1_prt_3_2_prt_4_3_prt_chr1          | table | elewis
 public | MockTable_1_prt_3_2_prt_4_3_prt_chr2          | table | elewis
 public | MockTable_1_prt_3_2_prt_4_3_prt_chr3          | table | elewis
 public | MockTable_1_prt_3_2_prt_4_3_prt_other         | table | elewis
 public | MockTable_1_prt_3_2_prt_5                     | table | elewis
 public | MockTable_1_prt_3_2_prt_5_3_prt_chr1          | table | elewis
 public | MockTable_1_prt_3_2_prt_5_3_prt_chr2          | table | elewis
 public | MockTable_1_prt_3_2_prt_5_3_prt_chr3          | table | elewis
 public | MockTable_1_prt_3_2_prt_5_3_prt_other         | table | elewis
 public | MockTable_1_prt_3_2_prt_extra                 | table | elewis
 public | MockTable_1_prt_3_2_prt_extra_3_prt_chr1      | table | elewis
 public | MockTable_1_prt_3_2_prt_extra_3_prt_chr2      | table | elewis
 public | MockTable_1_prt_3_2_prt_extra_3_prt_chr3      | table | elewis
 public | MockTable_1_prt_3_2_prt_extra_3_prt_other     | table | elewis
 public | MockTable_1_prt_extra                         | table | elewis
 public | MockTable_1_prt_extra_2_prt_2                 | table | elewis
 public | MockTable_1_prt_extra_2_prt_2_3_prt_chr1      | table | elewis
 public | MockTable_1_prt_extra_2_prt_2_3_prt_chr2      | table | elewis
 public | MockTable_1_prt_extra_2_prt_2_3_prt_chr3      | table | elewis
 public | MockTable_1_prt_extra_2_prt_2_3_prt_other     | table | elewis
 public | MockTable_1_prt_extra_2_prt_3                 | table | elewis
 public | MockTable_1_prt_extra_2_prt_3_3_prt_chr1      | table | elewis
 public | MockTable_1_prt_extra_2_prt_3_3_prt_chr2      | table | elewis
 public | MockTable_1_prt_extra_2_prt_3_3_prt_chr3      | table | elewis
 public | MockTable_1_prt_extra_2_prt_3_3_prt_other     | table | elewis
 public | MockTable_1_prt_extra_2_prt_4                 | table | elewis
 public | MockTable_1_prt_extra_2_prt_4_3_prt_chr1      | table | elewis
 public | MockTable_1_prt_extra_2_prt_4_3_prt_chr2      | table | elewis
 public | MockTable_1_prt_extra_2_prt_4_3_prt_chr3      | table | elewis
 public | MockTable_1_prt_extra_2_prt_4_3_prt_other     | table | elewis
 public | MockTable_1_prt_extra_2_prt_5                 | table | elewis
 public | MockTable_1_prt_extra_2_prt_5_3_prt_chr1      | table | elewis
 public | MockTable_1_prt_extra_2_prt_5_3_prt_chr2      | table | elewis
 public | MockTable_1_prt_extra_2_prt_5_3_prt_chr3      | table | elewis
 public | MockTable_1_prt_extra_2_prt_5_3_prt_other     | table | elewis
 public | MockTable_1_prt_extra_2_prt_extra             | table | elewis
 public | MockTable_1_prt_extra_2_prt_extra_3_prt_chr1  | table | elewis
 public | MockTable_1_prt_extra_2_prt_extra_3_prt_chr2  | table | elewis
 public | MockTable_1_prt_extra_2_prt_extra_3_prt_chr3  | table | elewis
 public | MockTable_1_prt_extra_2_prt_extra_3_prt_other | table | elewis
 ```
