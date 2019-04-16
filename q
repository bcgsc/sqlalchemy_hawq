[1mdiff --git a/setup.py b/setup.py[m
[1mindex af708c0..f24d020 100644[m
[1m--- a/setup.py[m
[1m+++ b/setup.py[m
[36m@@ -7,7 +7,7 @@[m [mINSTALL_REQS = ['sqlalchemy>=1.2.12[postgresql]', 'psycopg2-binary', 'sqlalchemy[m
 DOCUMENTATION_REQS = ['sphinx'][m
 [m
 # Dependencies required only for running tests[m
[31m-TEST_REQS = ['pytest', 'pytest-cov', 'mock', 'pytest-xdist'][m
[32m+[m[32mTEST_REQS = ['pytest>=4.4.0', 'pytest-cov', 'mock', 'pytest-xdist'][m
 [m
 # Dependencies required for deploying to an index server[m
 DEPLOYMENT_REQS = ['twine'][m
[1mdiff --git a/test/test_live_connection.py b/test/test_live_connection.py[m
[1mindex cefbcfe..c30004e 100644[m
[1m--- a/test/test_live_connection.py[m
[1m+++ b/test/test_live_connection.py[m
[36m@@ -2,14 +2,16 @@[m
 Defines tests that require a live db connection.[m
 """[m
 from sqlalchemy.ext.declarative import declarative_base[m
[31m-from sqlalchemy import Column, Integer[m
[32m+[m[32mfrom sqlalchemy import Column, Integer, Table, text[m
 from sqlalchemy.exc import StatementError[m
 from sqlalchemy.testing.suite import fixtures[m
 from sqlalchemy.testing import assert_raises[m
[31m-from sqlalchemy.orm import Session[m
[31m-[m
[32m+[m[32mfrom sqlalchemy.orm import Session, sessionmaker[m
[32m+[m[32mfrom sqlalchemy import create_engine, MetaData[m
[32m+[m[32mfrom sqlalchemy.schema import CreateTable[m
 [m
 from sqlalchemy_hawq.point import Point[m
[32m+[m[32mfrom sqlalchemy_hawq.ddl import CreateView[m
 [m
 [m
 class TestWithLiveConnection(fixtures.DeclarativeMappedTest):[m
[36m@@ -131,6 +133,7 @@[m [mclass TestWithLiveConnection(fixtures.DeclarativeMappedTest):[m
             except Exception as e:[m
                 session.close()[m
                 raise e[m
[32m+[m
         assert_raises(StatementError, func)[m
 [m
     def test_point_type_insert_select_mixed_type_2(self):[m
[36m@@ -151,4 +154,59 @@[m [mclass TestWithLiveConnection(fixtures.DeclarativeMappedTest):[m
             except Exception as e:[m
                 session.close()[m
                 raise e[m
[32m+[m
         assert_raises(StatementError, func)[m
[32m+[m
[32m+[m
[32m+[m[32mclass TestViewsWithLiveConnection(fixtures.TestBase):[m
[32m+[m
[32m+[m[32m    # ensures this test class is run when option --backend-only is specified[m
[32m+[m[32m    __backend__ = True[m
[32m+[m[32m    engine = create_engine(fixtures.config.db_url)[m
[32m+[m[32m    metadata = MetaData()[m
[32m+[m
[32m+[m[32m    def teardown(self):[m
[32m+[m[32m        import pdb[m
[32m+[m
[32m+[m[32m        pdb.set_trace()[m
[32m+[m[32m        print('hello')[m
[32m+[m
[32m+[m[32m    def test_create_view(self):[m
[32m+[m[32m        """[m
[32m+[m[32m        Checks view is created and can be queried[m
[32m+[m[32m        """[m
[32m+[m[32m        import pdb[m
[32m+[m
[32m+[m[32m        pdb.set_trace()[m
[32m+[m[32m        session = Session(bind=self.engine)[m
[32m+[m
[32m+[m[32m        source_table = Table([m
[32m+[m[32m            'source_table',[m
[32m+[m[32m            self.metadata,[m
[32m+[m[32m            Column('notid', Integer, autoincrement=False),[m
[32m+[m[32m            Column('test', Integer),[m
[32m+[m[32m        )[m
[32m+[m
[32m+[m[32m        view_table = Table([m
[32m+[m[32m            'view_table',[m
[32m+[m[32m            self.metadata,[m
[32m+[m[32m            Column('notid', Integer, primary_key=True),[m
[32m+[m[32m            Column('test', Integer),[m
[32m+[m[32m        )[m
[32m+[m
[32m+[m[32m        # pdb.set_trace()[m
[32m+[m[32m        definition = text('select * from {}'.format(source_table))[m
[32m+[m[32m        stmt = CreateView(view_table, definition, or_replace=True)[m
[32m+[m
[32m+[m[32m        # pdb.set_trace()[m
[32m+[m[32m        source1 =[m
[32m+[m[32m        session.add(source_table).params(notid=1, test=2)[m
[32m+[m[32m        session.commit()[m
[32m+[m[32m        session.execute(str(stmt.compile()).strip())[m
[32m+[m[32m        session.commit()[m
[32m+[m
[32m+[m[32m        # pdb.set_trace()[m
[32m+[m[32m        x = session.query('view_table')[m
[32m+[m[32m        assert False == x[m
[32m+[m
[32m+[m[32m#TODO: mapping is missing[m
