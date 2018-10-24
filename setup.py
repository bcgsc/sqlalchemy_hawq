
from setuptools import setup, find_packages

# Dependencies required to use your package
INSTALL_REQS = []

# Dependencies required only for building the documentation
DOCUMENTATION_REQS = [
    'sphinx'
]

# Dependencies required only for running tests
TEST_REQS = [
    'pytest',
    'pytest-runner',
    'pytest-cov',
    'twine'
]

# Dependencies required for deploying to an index server
DEPLOYMENT_REQS = [
    'twine'
]


setup(
    name='hawq_sqlalchemy',
    version='0.1.0',
    packages=find_packages(),
    install_requires=INSTALL_REQS,
    extras_require={
        'docs': DOCUMENTATION_REQS,
        'dev': TEST_REQS + DEPLOYMENT_REQS + DOCUMENTATION_REQS
    },
    python_requires='>=3',
    author_email='creisle@bcgsc.ca',
    dependency_links=[],
    test_suite='tests',
    tests_require=TEST_REQS,
    entry_points={'console_scripts': []}
)
