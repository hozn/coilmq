#!python
"""
Entrypoint for starting the application.
"""
from ConfigParser import ConfigParser
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
import logging
from optparse import OptionParser

from coilmq.config import config as global_config, init_config, resolve_name
from coilmq.topic import TopicManager
from coilmq.queue import QueueManager
 
from coilmq.server.socketserver import ThreadedStompServer

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
    return server

def main():
    """ Start the socket server. """

    parser = OptionParser()
    parser.add_option("-c", "--config", dest="config_file",
                      help="Read configuration from FILE. (CLI options override config file.)", metavar="FILE")
    
    parser.add_option("-b", "--host", dest="listen_addr",
                      help="Listen on specified address (default 0.0.0.0)", metavar="ADDR")
    
    parser.add_option("-p", "--port", dest="listen_port",
                      help="Listen on specified port (default 61613)", type="int", metavar="PORT")
    
    # TODO: Add other options
    #        - daemonize
    #        -- pidfile
    #        -- guid
    #        -- uid
    
    (options, args) = parser.parse_args()
    
    init_config(options.config_file)
    
    if options.listen_addr is not None:
        global_config.set('coilmq', 'listen_addr', options.listen_addr)
        
    if options.listen_port is not None:
        global_config.set('coilmq', 'listen_port', options.listen_port)
    
    server = server_from_config()
    logger().info("Stomp server listening on %s:%s" % server.server_address)
    server.serve_forever()
    
if __name__ == '__main__':
    main()