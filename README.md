
# Sqlalchemy Hawq

This is a custom dialect for using SQLAlchemy with a [HAWQ](http://hawq.apache.org/docs/userguide/2.3.0.0-incubating/tutorial/overview.html)
database.

It extends the Postgresql dialect.

Features include:
- Hawq options for 'CREATE TABLE' statements
- a point class
- a modified 'DELETE' statement for compatibility with SQLAlchemy's test suite

Unless specificaly overridden, any functionality in SQLAlchemy's Postgresql dialect
is also available.


## Getting Started

### Install (For developers)


clone this repository

```bash
git clone https://creisle@svn.bcgsc.ca/bitbucket/scm/vdb/sqlalchemy_hawq.git
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

sqlalchemy_hawq incorporates the standard SQLAlchemy test suite as well as some tests
of its own. Run them all as follows:

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

For tests that use a live db connection, user running the tests must be able to create and drop
tables on the db provided. Also, many of the tests require that there are pre-existing schemas
'test_schema' and 'test_schema_2' on the db. The test suite can be run without them but the tests
will fail.

See https://github.com/zzzeek/sqlalchemy/blob/master/README.unittests.rst and
https://github.com/zzzeek/sqlalchemy/blob/master/README.dialects.rst for more information on
test configuration. Note that no default db is stored in sqlalchemy_hawq's setup.cfg.



## Using in an SQLAlchemy project

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

For sqlalchemy's instructions on how to use their engine, see
https://docs.sqlalchemy.org/en/13/core/engines.html.


### Hawq-specific table arguments

Hawq specific table arguments are also supported (Not all features are supported yet)

| Argument | Type | Example | Notes |
|----------|------|---------|-------|
| hawq_distributed_by | str | `'column_name'` | |
| hawq_partition_by | RangePartition or ListPartition | `ListPartition('chrom', {'chr1': '1', 'chr2':'2', 'chr3':'3'}, [RangeSubpartition('year', 2002, 2012, 1), RangeSubpartition('month', 1, 13, 1),])` | Does not currently support range partitioning on dates |
| hawq_apppendonly | bool | `True` | |
| hawq_orientation | str | `'ROW'` | expects one of `{'ROW', 'PARQUET'}` |
| hawq_compresstype | str | `'ZLIB'` | expects one of `{'ZLIB', 'SNAPPY', 'GZIP', 'NONE'}` |
| hawq_compresslevel | int | `0` | expects an integer between 0-9 |

---

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

### Using partitions

See https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/ddl/ddl-partition.html for an
extended discussion of how partitions work in Hawq.

Basically, partitioning divides a table into several smaller tables on the value of one or more
columns, in order to reduce search time on those columns. The parent table can then be queried/added
to without any further reference to the partitions, as Hawq handles all the parent-partition
interactions.

Using sqlalchemy-hawq syntax to define a partition:

```python
class ExampleTable(Base):
    __tablename__ = 'example_table'

    __table_args__ = {
        'hawq_distributed_by': 'attr1'
        'hawq_appendonly': 'True'
    }

    attr1 = Column(Integer())
    attr2 = Column(Integer())

```

The SQL output:





The resulting tables:









Partition arguments are

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

Note that the params are the same for the Subpartitions are for the Partitions, except that
Subpartitions do not have a nested subpartition array.

Partition level is determined by the order of the subpartitions in the subpartition array.
