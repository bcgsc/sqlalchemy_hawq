
# Hawq Sqlalchemy

This is a custom dialect for using SQLAlchemy with a [HAWQ](http://hawq.apache.org/docs/userguide/2.3.0.0-incubating/tutorial/overview.html)
database.

## Getting Started

### Install (For developers)

clone this repository

```bash
git clone https://creisle@svn.bcgsc.ca/bitbucket/scm/vdb/hawq_sqlalchemy.git
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

```bash
pytest tests
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
|--|--|--|--|
| hawq_distributed_by | str | `'column_name'` | |
| hawq_partition_by | tuple of str and dict by str | `('column_name', {'part1': 1, 'part2': 2})` | Currently only supports parition by list and will always create a default partition of other |
| hawq_apppendonly | bool | `True` | |
| hawq_orientation | str | `'ROW'` | expects one of `{'ROW', 'PARQUET'}` |
| hawq_compresstype | str | `'ZLIB'` | expects one of `{'ZLIB', 'SNAPPY', 'GZIP', 'NONE'}` |
| hawq_compresslevel | int | 0 | expects an integer between 0-9 |

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
