===========
 Changelog
===========

..
    Unreleased
    ----------
    **Added**
    **Changed**
    **Deprecated**
    **Removed**
    **Fixed**
    **Security**

Unreleased
----------

**Changed**

- dropped support for Python 2.7 through Python 3.7 (:pr:`37`)
- refactor tests to idiomatic pytest style (:pr:`66`)
- use Sphinx markup for documentation (:pr:`52`)
- format project code with ruff (:issue:`58`)

**Fixed**

- CoilMQ rejects CONNECT without accept-version header (:issue:`29`)
- subscription id not returned to client (:issue:`25`)
- error in ACK processing in version 1.1 protocol (:issue:`34`)
- STOMP commands and header names should be case sensitive (:issue:`23`)
- remove ``pkg_resources`` dependency (:issue:`39`)
- CI builds are no longer running (:issue:`31`)
- command line argument parsing with click 8 (:pr:`28`)
- large messages corrupt `~coilmq.util.frames.FrameBuffer` and hang server (:issue:`44`)
- heart-beat mechanism makes CoilMQ unusable with heartbeats enabled (:issue:`26`)
- `~coilmq.scheduler.QueuePriorityScheduler` should be an abstract base class (:issue:`53`)
- use `importlib.resources` to load the default configuration (:issue:`56`)
- pytest treats ``TestStompServer`` and ``TestStompClient`` as test cases (:issue:`62`)
- coilmq sends ``CONNECTED`` twice when handling STOMP 1.0 ``CONNECT`` (:issue:`32`)

v1.0.1
------

**Fixed**

- ImportError: No module named 'pwd' (:issue:`20`)
- replace `NotImplemented` with `NotImplementedError` (:pr:`18`)

v1.0.0
------

CoilMQ moved from Google Code to GitHub during the development of v1.0.0.  This section
(and those above) refers to issues and PRs opened against CoilMQ on GitHub.  Sections
for older releases (below) refer to issues opened against the `project on Google Code`_.

.. _project on Google Code: https://code.google.com/archive/p/coilmq/issues

**Added**

- Python 3 support (:pr:`3`)
- STOMP 1.1 support (:pr:`12`)
- STOMP 1.2 support (:pr:`13`)
- support for ``STOMP`` command (:pr:`14`)
- `QueueStore` implementation backed by a Redis database (:pr:`15`)

**Fixed**

- show correct exception on TCPServer init error (:pr:`1`)

v0.6.1
------
* Error with one subscriber causes topic messages not to be delivered to
  other subscribers (issue 33).
* Fixed error in some circumstances when clearing pending transaction
  frames with commit/abort (issue 30).
* Fixed incorrect default address in help (issue 29).

v0.6.0
------
* Added a new diagnostic thread that will run when --debug option
  is passed on the command line.
* Added method to QueueManager API  to support tracking subscriber count.
* Improved unit and functional test coverage of storage engines.
* Fixed bug in engine.commit() and updated tests to catch previous
  failure (issue 28).

v0.5.0
------
* Added support for RECEIPT header and server messages (issue 26).

v0.4.4
------
* Fixed packaging (MANIFEST.in) to include defaults.cfg and config.cfg-sample
  (issue 23).
* Fixed socket recv loop to appropriately handle client DISCONNECT messages
  (issue 24).

v0.4.3
------
* Fixed bug in requeuing of pending frames when client is disconnected
  (issue 22).
* Fixed bug in unit test for dbm on windows (issue 21).

v0.4.2
------
* Added allow_socket_reuse (SO_REUSEADDR) option to SocketServer subclass
  to avoid having to wait to restart server after unclean client
  disconnect.

v0.4.1
------
* Added a changelog ;)
* Added socket timeouts so that the server can be interrupted (e.g. CTRL-C)
