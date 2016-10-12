"""
Simple, lightweight, and easily extensible STOMP message broker.
"""
import os.path

from setuptools import setup, find_packages
from setuptools.command.test import test


class PyTest(test):
    def finalize_options(self):
        test.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        pytest.main(self.test_args)


version = '1.0.1'

long_description = """
The provided server implementation for CoilMQ uses the Python SocketServer
libraries; however, CoilMQ is only loosely coupled to this server
implementation.  It could be used with other socket implementations.

The CoilMQ core classes and bundled storage implementations are built to be
thread-safe.


"""


def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()


setup(
    name='CoilMQ',
    version=version,
    description=__doc__,
    long_description=long_description + read('docs/news.txt'),
    keywords='stomp server broker messaging',
    license='Apache',
    author='Hans Lellelid',
    author_email='hans@xmpl.org',
    url='https://github.com/hozn/coilmq',
    packages=find_packages(exclude=['ez_setup', 'distribute_setup', 'tests', 'tests.*']),
    package_dir={'coilmq':  'coilmq'},
    package_data={'coilmq': ['config/*.cfg*', 'tests/resources/*']},
    zip_safe=False,  # We use resource_filename for logging configuration and some unit tests.
    include_package_data=True,
    tests_require=read('requirements/test.txt').splitlines(),
    cmdclass={'test': PyTest},
    install_requires=read('requirements/prod.txt').splitlines(),
    extras_require={
        'sqlalchemy': ['SQLAlchemy'],
        'redis': ['redis']
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Communications",
        "Topic :: System :: Distributed Computing",
        "Topic :: System :: Networking"
    ],
    entry_points="""
    [console_scripts]
    coilmq = coilmq.start:main
    """,
)
