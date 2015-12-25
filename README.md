CoilMQ
======

[![Build Status](https://travis-ci.org/hozn/coilmq.svg?branch=master)](https://travis-ci.org/hozn/coilmq)
[![Coverage Status](https://coveralls.io/repos/hozn/coilmq/badge.svg?branch=master&service=github)](https://coveralls.io/github/hozn/coilmq?branch=master)
[![PyPI downloads](https://img.shields.io/pypi/dm/coilmq.svg)](https://badge.fury.io/py/coilmq)


CoilMQ is a simple, configurable STOMP message broker (aka STOMP server) written in Python. This project is inspired by the simple design of the Ruby [stompserver project](http://stompserver.rubyforge.org/) by Patrick Hurley and Lionel Bouton. The goal of this project is to provide a well-tested and well-documented Python implementation with a good set of built-in functionality and numerous paths for extensibility.

Now!
====

    $ pip install CoilMQ
    $ coilmq -b 0.0.0.0 -p 61613

Jump over to [Getting Started Guide](https://github.com/hozn/coilmq/wiki/Getting-Started) for the slightly longer version.

Current features
================
* Support for STOPM v{[1.0](http://stomp.github.io/stomp-specification-1.0.html), [1.1](http://stomp.github.io/stomp-specification-1.1.html), [1.2](http://stomp.github.io/stomp-specification-1.2.html)}
* Works on Python {2.7, 3.4, 3.5}
* {Redis, Memory, RDBM} message store options
* Extendable via custom components

Status and Goals
================
The project is currently in beta state and the tip branch in repository should always be considered unstable. Milestones (of varying levels of stability) will be signified by releases.

At a high-level, this project aims to:

* Provide a correct and functional STOMP implementation
* Be well documented.
* Be easy to extend. (e.g. write new auth backend, queue storage)
* Be well tested.

Take a look at the [Roadmap](https://github.com/hozn/coilmq/wiki/Roadmap) for more details.
