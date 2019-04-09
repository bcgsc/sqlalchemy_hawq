
# Hawq Sqlalchemy

This is a custom dialect for using SQLAlchemy with a [HAWQ](http://hawq.apache.org/docs/userguide/2.3.0.0-incubating/tutorial/overview.html)
database.

## Getting Started

### Install (For developers)


clone this repository

```bash
git clone https://creisle@svn.bcgsc.ca/bitbucket/scm/dat/hawq_sqlalchemy.git
cd hawq_sqlalchemy
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

The whole test package can be run using:

```bash
pytest test --dburi hawq://$YOURUSERNAME:$YOURHAWQPASS@hdp-master02.hadoop.bcgsc.ca:5432/test_refactor
```
where your user name and hawq pass are the credentials for connecting to the test_refactor db.

To run only the custom (local) tests, use the option --custom-only:
```bash
pytest test --dburi hawq://$YOURUSERNAME:$YOURHAWQPASS@hdp-master02.hadoop.bcgsc.ca:5432/test_refactor --custom-only
```

To run only the SQLAlchemy test suite, use the option --sqla-only:
```bash
pytest test --dburi hawq://$YOURUSERNAME:$YOURHAWQPASS@hdp-master02.hadoop.bcgsc.ca:5432/test_refactor --sqla-only
```

To run only the unit (local) tests, use the option --unit-only:
```bash
pytest test --dburi hawq://$YOURUSERNAME:$YOURHAWQPASS@hdp-master02.hadoop.bcgsc.ca:5432/test_refactor --unit-only
```


## Using in an SQLAlchemy project

Add hawq_sqlalchemy to your dependencies and install.

```bash
pip install hawq_sqlalchemy
```

Then the plugin can be used like any other engine

```python
from sqlalchemy import create_engine

engine = create_engine('hawq://....')
```

Hawq specific table arguments are also supported (Not all features are supported yet)

| Argument | Type | Example | Notes |
|----------|------|---------|-------|
| hawq_distributed_by | str | `'column_name'` | |
| hawq_partition_by | RangePartition or ListPartition | `ListPartition('chrom', {'chr1': '1', 'chr2':'2', 'chr3':'3'}, [RangeSubpartition('year', 2002, 2012, 1), RangeSubpartition('month', 1, 13, 1),])` | Does not currently support range partitioning on dates |
| hawq_apppendonly | bool | `True` | |
| hawq_orientation | str | `'ROW'` | expects one of `{'ROW', 'PARQUET'}` |
| hawq_compresstype | str | `'ZLIB'` | expects one of `{'ZLIB', 'SNAPPY', 'GZIP', 'NONE'}` |
| hawq_compresslevel | int | `0` | expects an integer between 0-9 |



Partition arguments are 
RangePartition(column_name=str, start=int, end=int, every=int, subpartitions=[]) or ListPartition(column_name=str, columns=dict{name_of_partition=str:value_to_partition_on=str}, subpartitions=[]),  where subpartitions is an array of RangeSubpartition and ListSubpartition. 

Subpartitions expect the same params as Partitions but without a nested subpartition array. 

Partition level is determined by the order of the subpartitions in the subpartition array. 


---

An example of declarative table syntax with hawq table arguments

```python
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Text

Base = declarative_base()


class ExampleTable(Base):
    __tablename__ = 'example_table'

    __table_args__ = {
        'hawq_distributed_by': 'attr1'
    }

    attr1 = Column(Text())
```
