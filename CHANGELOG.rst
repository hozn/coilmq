===========
 Changelog
===========

Unreleased
----------
* Removed uses of `pkg_resources` (issue 39)
* Removed support for Python 2.7 through Python 3.7 (MR 37)
* Fixed a defect in `coilmq.util.frames.FrameBuffer.extract_frame`: on
  an `IncompleteFrame` error, this method set the buffer's next write
  index to the read index (rather than the end of the buffer) causing
  the subsequent `FrameBuffer.append()` call(s) to overwrite the
  beginning of the frame and hanging the server (issue 44)
* Fixed defects in CoilMQ's heart-beating implementation (issue 26)
  * stop heart-beating threads when a connection closes
  * treat any incoming traffic as a heart-beat
  * tolerate heart-beats arriving as late as twice the requested interval

1.0.1
-----

1.0.0
-----

0.6.1
-----
* Error with one subscriber causes topic messages not to be delivered to 
  other subscribers (issue 33).
* Fixed error in some circumstances when clearing pending transaction 
  frames with commit/abort (issue 30).
* Fixed incorrect default address in help (issue 29).

0.6.0
-----
* Added a new diagnostic thread that will run when --debug option
  is passed on the commandline.
* Added method to QueueManager API  to support tracking subscriber count. 
* Improved unit and functional test coverage of storage engines.
* Fixed bug in engine.commit() and updated tests to catch previous 
  failure (issue 28).

0.5.0
-----
* Added support for RECEIPT header and server messages (issue 26). 

0.4.4
-----

* Fixed packaging (MANIFEST.in) to include defaults.cfg and config.cfg-sample
  (issue 23).
* Fixed socket recv loop to appropriately handle client DISCONNECT messages
  (issue 24).

0.4.3
-----
* Fixed bug in requeuing of pending frames when client is disconnected
  (issue 22).
* Fixed bug in unit test for dbm on windows (issue 21).

0.4.2
-----
* Added allow_socket_reuse (SO_REUSEADDR) option to SocketServer subclass
  to avoid having to wait to restart server after unclean client 
  disconnect. 

0.4.1
-----
* Added a changelog ;)
* Added socket timeouts so that the server can be interrupted (e.g. CTRL-C)
