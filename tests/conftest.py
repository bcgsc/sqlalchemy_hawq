import pytest
from sqlalchemy.engine.url import URL
import hawq_sqlalchemy
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import create_engine

from sqlalchemy.orm import configure_mappers
from sqlalchemy.schema import CreateSchema
def pytest_addoption(parser):
    parser.addoption("--username", action="store", default=None, help="gateway username")
    parser.addoption("--password", action="store", default=None, help="password for gateway user")
    parser.addoption("--echo", action="store", default=None, help="echo commands to db")


@pytest.fixture(scope='session')
def username(request):
    un = request.config.getoption("--username")
    if un is None:
        print("Skipped test requiring username or password")
        pytest.skip()
    return un

@pytest.fixture(scope='session')
def password(request):
    passw = request.config.getoption("--password")
    if passw is None:
        print("Skipped test requiring username or password")
        pytest.skip()
    return passw

@pytest.fixture(scope='session')
def echo_sql(request):
    echo = request.config.getoption("--echo")
    if echo is None or echo is False:
        return False
    return True


@pytest.fixture(scope='session')
def test_engine(request, username, password, echo_sql):
    host = 'hdp-master02.hadoop.bcgsc.ca'
    port = '5432'
    database = 'test_refactor'
    drivername = 'hawq'
    url = URL(host=host,
                database=database,
                username=username,
                password=password,
                drivername=drivername,
                port=port)

    engine = create_engine(url, echo=echo_sql)

    configure_mappers()

    schemaname = 'testschema'

    try:
        engine.execute("drop schema " + schemaname + " cascade;")
        print("Schema " + schemaname + " existed and was dropped before creating")
    except (ProgrammingError) as prog_e:
        does_not_exist = 'schema \"' + schemaname + '\" does not exist\n'
        if prog_e.orig.args[0] != does_not_exist:
            print("Unexpected error:", prog_e.orig)
            return
    engine.execute(CreateSchema(schemaname))
    engine.execute("GRANT ALL PRIVILEGES ON SCHEMA " + schemaname + " TO refactor_admin;")

    yield engine
    """ try:
        engine.execute("drop schema " + schemaname + " cascade;")
    except (ProgrammingError) as prog_e:
        does_not_exist = "schema \'" + schemaname + "\' does not exist\n"
        if prog_e.orig.args[0] != does_not_exist:
            print("Unexpected error:", prog_e.orig)
            return """
    engine.dispose()

