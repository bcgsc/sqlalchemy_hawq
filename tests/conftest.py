"""
Defines pytest command line arguments and fixtures that can be used throughout tests.
"""
import pytest
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import create_engine
from sqlalchemy.orm import configure_mappers, sessionmaker
from sqlalchemy.schema import CreateSchema
from sqlalchemy.engine.url import URL


def pytest_addoption(parser):
    """
    Defines possible command line arguments
    """
    parser.addoption("--username", action="store", default=None, help="gateway username")
    parser.addoption("--password", action="store", default=None, help="password for gateway user")
    parser.addoption("--echo", action="store", default=False, help="echo commands to db")


@pytest.fixture(scope='session')
def username(request):
    """
    Returns username passed as command line arg
    """
    usern = request.config.getoption("--username")
    if usern is None:
        print("Skipped test requiring username or password")
        pytest.skip()
    return usern


@pytest.fixture(scope='session')
def password(request):
    """
    Returns password passed as command line arg
    """
    passw = request.config.getoption("--password")
    if passw is None:
        print("Skipped test requiring username or password")
        pytest.skip()
    return passw


@pytest.fixture(scope='session')
def echo_sql(request):
    """
    Returns 'echo' passed as command line arg
    """
    echo = request.config.getoption("--echo")
    if echo is False:
        return False
    return True


@pytest.fixture(scope='session')
def schemaname(request):
    return 'testschema'


@pytest.fixture(scope='session')
def test_engine(request, username, password, echo_sql, schemaname):
    """
    Returns engine connected to hawq database with schema testschema initialized
    """
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
    engine.execute("drop schema " + schemaname + " cascade;")
    engine.dispose()


@pytest.fixture(scope='session')
def SessionFactory(test_engine):
    Session = sessionmaker(bind=test_engine)
    yield Session
    Session.close_all()
