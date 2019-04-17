from setuptools import setup

# Dependencies required to use your package
INSTALL_REQS = ['sqlalchemy>=1.2.12[postgresql]', 'psycopg2-binary']

# Dependencies required only for building the documentation
DOCUMENTATION_REQS = ['sphinx']

# Dependencies required only for running tests
TEST_REQS = ['pytest', 'pytest-cov', 'mock', 'pytest-xdist']

# Dependencies required for deploying to an index server
DEPLOYMENT_REQS = ['twine', 'wheel']

PACKAGES = ['test', 'sqlalchemy_hawq']

setup(
    name='sqlalchemy_hawq',
    version='0.2.0',
    packages=PACKAGES,
    install_requires=INSTALL_REQS,
    extras_require={
        'docs': DOCUMENTATION_REQS,
        'dev': TEST_REQS + DEPLOYMENT_REQS + DOCUMENTATION_REQS,
        'test': TEST_REQS,
        'deploy': DEPLOYMENT_REQS,
    },
    python_requires='>=3',
    author_email='creisle@bcgsc.ca',
    dependency_links=[],
    test_suite='test',
    tests_require=TEST_REQS,
    entry_points={
        'sqlalchemy.dialects': [
            'hawq = sqlalchemy_hawq.dialect:HawqDialect',
            'hawq+psycopg2 = sqlalchemy_hawq.dialect:HawqDialect',
        ]
    },
)
