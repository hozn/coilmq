"""
Configuration support functionality.

The global C{config} object (C{ConfigParser.SafeConfigParser} instance) is initialized
with default configuration from the defaults.cfg file, which is located in this package.
In order to ensure that the config contains custom values, you must call the C{init_config}
function during application initialization:

from coilmq.config import config, init_config
init_config('/path/to/config.cfg')

config.getint('listen_port')
"""
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
import os.path
import logging
import logging.config
import ConfigParser

from pkg_resources import resource_stream, resource_filename

config = ConfigParser.SafeConfigParser()
config.readfp(resource_stream(__name__, 'defaults.cfg'))

def init_config(config_file):
    """
    Initialize the configuration from a config file.
    
    The values in config_file will override those already loaded from the default
    configuration file (defaults.cfg, in current package).
    
    @param config_file: The path to a configuration file.
    @type config_file: C{str}
    
    @raise ValueError: if the specified config_file could not be read.  
    """
    global config
    
    if config_file and os.path.exists(config_file):
        logging.config.fileConfig(config_file)
        read = config.read([config_file])
        if not read:
            raise ValueError("Could not read configuration from file: %s" % config_file)
    else:
        logging.config.fileConfig(resource_filename(__name__, 'defaults.cfg'))

def resolve_name(name):
    """
    Resolve a dotted name to some object (usually class, module, or function).
    
    Supported naming formats include:
        1. path.to.module:method
        2. path.to.module.ClassName
    
    >>> resolve_name('coilmq.store.memory.MemoryQueue')
    <class 'coilmq.store.memory.MemoryQueue'>
    >>> t = resolve_name('coilmq.store.dbm.make_dbm')
    >>> type(t)
    <type 'function'>
    >>> t.__name__
    'make_dbm'
    
    @param name: The dotted name (e.g. path.to.MyClass)
    @type name: C{str}
    
    @return: The resolved object (class, callable, etc.) or None if not found.
    """
    if ':' in name:
        # Normalize foo.bar.baz:main to foo.bar.baz.main
        # (since our logic below will handle that)
        name = '%s.%s' % tuple(name.split(':'))
        
    name = name.split('.')
    
    used = name.pop(0)
    found = __import__(used)
    for n in name:
        used = used + '.' + n
        try:
            found = getattr(found, n)
        except AttributeError:
            __import__(used)
            found = getattr(found, n)
            
    return found