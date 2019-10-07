import re


from setuptools import setup

# Dependencies required to use your package
INSTALL_REQS = ['sqlalchemy>=1.2.12[postgresql]', 'psycopg2-binary']

# Dependencies required only for building the documentation
DOCUMENTATION_REQS = []

# Dependencies required only for running tests
TEST_REQS = ['pytest>=4.4.0', 'pytest-cov', 'mock', 'pytest-xdist']

# Dependencies required for deploying to an index server
DEPLOYMENT_REQS = ['twine', 'wheel', 'm2r']

DEVELOPMENT_REQS = ['black', 'flake8']

PACKAGES = ['test', 'sqlalchemy_hawq']

try:
    import m2r

    long_description = m2r.parse_from_file('README.md')
    long_description = re.sub(r'.. code-block::.*', '.. code::', long_description)
except ImportError:
    with open('README.md', 'r') as f:
        long_description = f.read()

setup(
    name='sqlalchemy_hawq',
    version='1.0.1',
    packages=PACKAGES,
    install_requires=INSTALL_REQS,
    extras_require={
        'docs': DOCUMENTATION_REQS,
        'dev': TEST_REQS + DEPLOYMENT_REQS + DOCUMENTATION_REQS + DEVELOPMENT_REQS,
        'test': TEST_REQS,
        'deploy': DEPLOYMENT_REQS,
    },
    python_requires='>=3.6',
    author_email='creisle@bcgsc.ca',
    url='https://github.com/bcgsc/sqlalchemy_hawq',
    dependency_links=[],
    test_suite='test',
    tests_require=TEST_REQS,
    entry_points={
        'sqlalchemy.dialects': [
            'hawq = sqlalchemy_hawq.dialect:HawqDialect',
            'hawq+psycopg2 = sqlalchemy_hawq.dialect:HawqDialect',
        ]
    },
    classifiers=["License :: OSI Approved :: MIT License"],
    long_description=long_description,
    long_description_content_type='text/x-rst',
)
