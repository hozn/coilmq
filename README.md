CoilMQ
======

[![Build Status](https://travis-ci.org/hozn/coilmq.svg?branch=master)](https://travis-ci.org/hozn/coilmq)
[![Coverage Status](https://coveralls.io/repos/hozn/coilmq/badge.svg?branch=master&service=github)](https://coveralls.io/github/hozn/coilmq?branch=master)
[![PyPI version](https://badge.fury.io/py/coilmq.svg)](https://badge.fury.io/py/coilmq)


CoilMQ is a simple, configurable STOMP message broker (aka STOMP server) written in Python. This project is inspired by the simple design of the Ruby [stompserver project](http://rubyforge.org/projects/stompserver/) by Patrick Hurley and Lionel Bouton. The goal of this project is to provide a well-tested and well-documented Python implementation with a good set of built-in functionality and numerous paths for extensibility.

Now!
====

    shell$ easy_install CoilMQ
    shell$ coilmq -b 0.0.0.0 -p 61613

Jump over to [Getting Started Guide](https://github.com/hozn/coilmq/wiki/Getting-Started) for the slightly longer version.

Current features
================
* STOPM v1.0
* Works on different versions of Python (2.7, 3.4, 3.5)
* Extendable via custom components

Status and Goals
================
The project is currently in beta state and the tip branch in repository should always be considered unstable. Milestones (of varying levels of stability) will be signified by releases.

At a high-level, this project aims to:

* Provide a correct and functional STOMP implementation (see note below on v1.1).
* Be well documented.
* Be easy to extend. (e.g. write new auth backend, queue storage)
* Be well tested.

Note that as of 3/31/2011 there is a version 1.1 of the STOMP protocol. Currently this is not supported by CoilMQ (but the plan is to add support).

Take a look at the [Roadmap](https://github.com/hozn/coilmq/wiki/Roadmap) for more details.
