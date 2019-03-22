
from setuptools import setup, find_packages

# Dependencies required to use your package
INSTALL_REQS = [
    'sqlalchemy>=1.2.12[postgresql]'
]

# Dependencies required only for building the documentation
DOCUMENTATION_REQS = [
    'sphinx'
]

# Dependencies required only for running tests
TEST_REQS = [
    'pytest',
    'pytest-cov'
]

# Dependencies required for deploying to an index server
DEPLOYMENT_REQS = [
    'twine'
]


setup(
    name='hawq_sqlalchemy',
    version='0.2.0',
    packages=find_packages(),
    install_requires=INSTALL_REQS,
    extras_require={
        'docs': DOCUMENTATION_REQS,
        'dev': TEST_REQS + DEPLOYMENT_REQS + DOCUMENTATION_REQS,
        'test': TEST_REQS
    },
    python_requires='>=3',
    author_email='creisle@bcgsc.ca',
    dependency_links=[],
    test_suite='tests',
    tests_require=TEST_REQS,
    entry_points={'sqlalchemy.dialects': [
        'hawq = hawq_sqlalchemy.dialect:HawqDialect',
        'hawq+psycopg2 = hawq_sqlalchemy.dialect:HawqDialect'
    ]}
)
