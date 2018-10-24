
# Hawq Sqlalchemy

This is a custom dialect for using SQLAlchemy with a [HAWQ](http://hawq.apache.org/docs/userguide/2.3.0.0-incubating/tutorial/overview.html)
database.


## Getting Started

### Install (For developers)

clone this repository

```
git clone https://creisle@svn.bcgsc.ca/bitbucket/scm/vdb/hawq_sqlalchemy.git
cd hawq_sqlalchemy
```

create a virtual environment

```
python3 -m venv venv
source venv/bin/activate
```

install the package and its development dependencies

```
pip install -e .[dev]
```

### Run Tests

```
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