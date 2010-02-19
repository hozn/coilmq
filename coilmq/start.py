#!python
"""
Entrypoint for starting the application.
"""
from __future__ import with_statement

import os
import logging
from optparse import OptionParser

try:
    daemon_support = True
    import daemon
    import signal
    import lockfile
except ImportError:
    daemon_support = False
    
from coilmq.config import config as global_config, init_config, resolve_name
from coilmq.topic import TopicManager
from coilmq.queue import QueueManager

from coilmq.exception import ConfigError
from coilmq.server.socketserver import ThreadedStompServer

__authors__ = ['"Hans Lellelid" <hans@xmpl.org>']
__copyright__ = "Copyright 2009 Hans Lellelid"
__license__ = """Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

logger = lambda: logging.getLogger(__name__)

def server_from_config(config=None, server_class=None, additional_kwargs=None):
    """
    Gets a configured L{coilmq.server.StompServer} from specified config.
    
    If `config` is None, global L{coilmq.config.config} var will be used instead.
    
    The `server_class` and `additional_kwargs` are primarily hooks for using this method
    from a testing environment.
    
    @param config: A C{ConfigParser.ConfigParser} instance with loaded config values.
    @type config: C{ConfigParser.ConfigParser}
    
    @param server_class: Which class to use for the server.  (This doesn't come from config currently.)
    @type server_class: C{class}
    
    @param additional_kwargs: Any additional args that should be passed to class.
    @type additional_kwargs: C{list}
    
    @return: The configured StompServer.
    @rtype: L{coilmq.server.StompServer}
    """
    global global_config
    if not config: config = global_config
    
    queue_store_factory = resolve_name(config.get('coilmq', 'qstore.factory'))
    subscriber_scheduler_factory = resolve_name(config.get('coilmq', 'scheduler.subscriber_priority_factory'))
    queue_scheduler_factory = resolve_name(config.get('coilmq', 'scheduler.queue_priority_factory'))
    
    if config.has_option('coilmq', 'auth.factory'):
        authenticator_factory = resolve_name(config.get('coilmq', 'authenticator_factory'))
        authenticator = authenticator_factory()
    else:
        authenticator = None
        
    server = ThreadedStompServer((config.get('coilmq', 'listen_addr'), config.getint('coilmq', 'listen_port')),
                                 queue_manager=QueueManager(store=queue_store_factory(),
                                                            subscriber_scheduler=subscriber_scheduler_factory(),
                                                            queue_scheduler=queue_scheduler_factory()),
                                 topic_manager=TopicManager(),
                                 authenticator=authenticator)
    logger().info("Created server:%r"% server)
    return server


def run_server(server):
    """
    Runs the specified server, catching any exceptions and handling server cleanup.
    """
    try:
        logger().info("Stomp server listening on %s:%s" % server.server_address)
        server.serve_forever()
    finally:
        # FIXME: Temporary!
        logger().info("Closing the server connection in serve() finally block.")
        server.server_close()
    
    
def main():
    """
    Main entry point for running a socket server from the commandline.
    
    This method will read in options from the commandline and call the L{config.init_config} method
    to get everything setup.  Then, depending on whether deamon mode was specified or not, 
    the process may be forked (or not) and the server will be started.
    """

    parser = OptionParser()
    parser.add_option("-c", "--config", dest="config_file",
                      help="Read configuration from FILE. (CLI options override config file.)", metavar="FILE")
    
    parser.add_option("-b", "--host", dest="listen_addr",
                      help="Listen on specified address (default 0.0.0.0)", metavar="ADDR")
    
    parser.add_option("-p", "--port", dest="listen_port",
                      help="Listen on specified port (default 61613)", type="int", metavar="PORT")

    parser.add_option("-d", "--daemon", action="store_true", dest="daemon", default=False,
                      help="Run server as a daemon (default False).")
    
    parser.add_option("--pidfile", dest="pidfile", default="./coilmq.pid",
                      help="The PID file to use.")
    
    parser.add_option("-u", "--uid", dest="uid",
                      help="The user/UID to use for daemon process.", metavar="UID")
    
    parser.add_option("-g", "--gid", dest="gid",
                      help="The group/GID to use for daemon process.", metavar="GID")
    
    parser.add_option("--umask", dest="umask",
                      help="Umask (octal) to apply for daemonized process.", metavar="MASK")
    
    (options, args) = parser.parse_args()
    
    # This probably needs to move into the daemon context block ...
    #
    #
    init_config(options.config_file)
    
    if options.listen_addr is not None:
        global_config.set('coilmq', 'listen_addr', options.listen_addr)
        
    if options.listen_port is not None:
        global_config.set('coilmq', 'listen_port', str(options.listen_port))
    
    if options.daemon:
        if not daemon_support:
            raise ConfigError("This environment/platform does not support running as daemon. (Are you running on *nix and did you install python-daemon package?)")
        else:
            
            context = daemon.DaemonContext()
            
            if options.uid is not None:
                context.uid = options.uid
                
            if options.gid is not None:
                context.gid = options.gid
                
            if options.pidfile is not None:
                context.pidfile = lockfile.FileLock(options.pidfile)
                #context.pidfile = options.pidfile
            
            if options.umask is not None:
                context.umask = int(options.umask, 8)

            server = None
            try:
                with context:
                    server = server_from_config()
                    logger().info("(Daemon) stomp server listening on %s:%s" % server.server_address)
                    server.serve_forever()
            except Exception, e:
                logger().error("This error ruined my day: %s" % e)
                logger().exception(e)
                raise
            finally:
                if server:
                    server.server_close()
    else:
        # Non-daemon version
        server = server_from_config()
        logger().info("Stomp server listening on %s:%s" % server.server_address)
        try:
            server.serve_forever()
        finally:
            server.server_close()
        
if __name__ == '__main__':
    try:
        main()
    except (KeyboardInterrupt, SystemExit):
        pass
    except Exception, e:
        logger().error("Server terminated due to error: %s" % e)
        logger().exception(e)