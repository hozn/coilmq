"""
CoilMQ is a STOMP message broker written in Python.

The provided server implementation for CoilMQ uses the Python SocketServer libraries;
however, CoilMQ is only loosely coupled to this server implementation.  It could be used
with other socket implementations.

Two things worth noting:

  1. The CoilMQ core classes and bundled storage implementations are designed to be thread-safe.

  2. While this project could be used with asynchronous frameworks like Twisted, it is not 
     explicitly designed to use the async patterns (e.g. using Twisted Deffered), which may 
     make it impractical to use this with slow/blocking storage mechanisms (e.g. using db 
     storage will cause all clients to block).
"""
try:
    from setuptools import setup, find_packages
except ImportError:
    from distribute_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

setup(
    name='CoilMQ',
    version='0.3.2',
    description='STOMP message broker',
    long_description=__doc__,
    keywords='stomp server broker',
    license='Apache',
    author='Hans Lellelid',
    author_email='hans@xmpl.org',
    url='http://code.google.com/p/coilmq',
    packages=find_packages(exclude=['ez_setup', 'distribute_setup', 'tests', 'tests.*']),
    package_data={'coilmq': ['config/*.cfg*', 'tests/resources/*']},
    zip_safe=False, # We use resource_filename for logging configuration and some unit tests.
    include_package_data=True,
    test_suite='nose.collector',
    tests_require=['nose', 'coverage'],
    install_requires=[
          'distribute',
          'stompclient==0.1',
    ],
    extras_require={
        'daemon': ['python-daemon'],
        'sqlalchemy': ['SQLAlchemy']
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    entry_points="""
    [console_scripts]
    coilmq = coilmq.start:main
    """,
)
