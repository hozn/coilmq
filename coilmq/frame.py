"""
A collection of STOMP frame convenience classes.

These classes are built around the C{stomper.Frame} class, primarily making it more 
convenient to construct frame instances.
"""
import stomper

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

class StompFrame(stomper.Frame):
    """ Convenience subclass of C{stomper.Frame} which provides a simpler
    construction API and automatically adds the content-length header, when applicable.
    
    """
    
    def __init__(self, cmd=None, headers=None, body=None):
        """
        Initialize new StompFrame with command, headers, and body.
        """
        if body is None:
            body = '' # Making it compatible w/ superclass
        if headers is None:
            headers = {}
        if cmd: # Only set if not None, since cmd prop setter will validate command is valid 
            self.cmd = cmd
        self.body = body
        self.headers = headers
    
    def __getattr__(self, name):
        """ Convenience way to return header values as if they're object attributes. 
        
        We replace '-' chars with '_' to make the headers python-friendly.  For example:
            
            frame.headers['message-id'] == frame.message_id
            
        >>> f = StompFrame(cmd='MESSAGE', headers={'message-id': 'id-here', 'other_header': 'value'}, body='')
        >>> f.message_id
        'id-here'
        >>> f.other_header
        'value'
        """
        if name.startswith('_'):
            raise AttributeError()
        
        try:
            return self.headers[name]
        except KeyError:
            # Try converting _ to -
            return self.headers.get(name.replace('_', '-'))
    
    def __eq__(self, other):
        """ Override equality checking to test for matching command, headers, and body. """
        return (isinstance(other, stomper.Frame) and 
                self.cmd == other.cmd and 
                self.headers == other.headers and 
                self.body == other.body)
    
    def __ne__(self, other):
        return not self.__eq__(other)
    
    def __repr__(self):
        return '<%s cmd=%s len=%d>' % (self.__class__.__name__, self.cmd, len(self.body))
    
class HeaderValue(object):
    """
    An descriptor class that can be used when a calculated header value is needed.
    
    This class is a descriptior, implementing  __get__ to return the calculated value.
    While according to  U{http://docs.codehaus.org/display/STOMP/Character+Encoding} there 
    seems to some general idea about having UTF-8 as the character encoding for headers;
    however the C{stomper} lib does not support this currently.
    
    For example, to use this class to generate the content-length header:
    
        >>> h = HeaderValue(calculator=lambda: len('asdf'))
        >>> str(h)
        '4'
        
    @ivar calc: The calculator function.
    @type calc: C{callable}
    """
    def __init__(self, calculator):
        """
        @param calculator: The calculator callable that will yield the desired value.
        @type calculator: C{callable}
        """
        if not callable(calculator):
            raise ValueError("Non-callable param: %s" % calculator)
        self.calc = calculator
    
    def __get__(self, obj, objtype):
        return self.calc()
    
    def __str__(self):
        return str(self.calc())
    
    def __set__(self, obj, value):
        self.calc = value
        
    def __repr__(self):
        return '<%s calculator=%s>' % (self.__class__.__name__, self.calc)

# ---------------------------------------------------------------------------------
# Server Frames
# ---------------------------------------------------------------------------------

class ConnectedFrame(StompFrame):
    """ A CONNECTED server frame (response to CONNECT).
    
    @ivar session: The (throw-away) session ID to include in response.
    @type session: C{str} 
    """
    def __init__(self, session):
        """
        @param session: The (throw-away) session ID to include in response.
        @type session: C{str}
        """
        StompFrame.__init__(self, 'CONNECTED', headers={'session': session})

class MessageFrame(StompFrame):
    """ A MESSAGE server frame. """
    
    def __init__(self, body=None):
        """
        @param body: The message body bytes.
        @type body: C{str} 
        """
        StompFrame.__init__(self, 'MESSAGE', body=body)
        self.headers['content-length'] = HeaderValue(calculator=lambda: len(self.body))
        
# TODO: Figure out what we need from ErrorFrame (exception wrapping?)
class ErrorFrame(StompFrame):
    """ An ERROR server frame. """
    
    def __init__(self, message, body=None):
        """
        @param body: The message body bytes.
        @type body: C{str} 
        """
        StompFrame.__init__(self, 'ERROR', body=body)
        self.headers['message'] = message
        self.headers['content-length'] = HeaderValue(calculator=lambda: len(self.body))
    
    def __repr__(self):
        return '<%s message=%r>' % (self.__class__.__name__, self.headers['message']) 
    
class ReceiptFrame(StompFrame):
    """ A RECEIPT server frame. """
    
    def __init__(self, receipt):
        """
        @param receipt: The receipt message ID.
        @type receipt: C{str}
        """
        StompFrame.__init__(self, 'RECEIPT', headers={'receipt': receipt})