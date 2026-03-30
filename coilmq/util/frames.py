import re
import logging
from collections import OrderedDict
import io
from itertools import starmap

SEND = 'SEND'
CONNECT = 'CONNECT'
MESSAGE = 'MESSAGE'
ERROR = 'ERROR'
CONNECTED = 'CONNECTED'
SUBSCRIBE = 'SUBSCRIBE'
UNSUBSCRIBE = 'UNSUBSCRIBE'
BEGIN = 'BEGIN'
COMMIT = 'COMMIT'
ABORT = 'ABORT'
ACK = 'ACK'
NACK = 'NACK'
STOMP_CMD = 'STOMP'
DISCONNECT = 'DISCONNECT'

VALID_COMMANDS = [MESSAGE, CONNECT, CONNECTED, ERROR, SEND,
                  SUBSCRIBE, UNSUBSCRIBE, BEGIN, COMMIT, ABORT, ACK, DISCONNECT, NACK, STOMP_CMD]

TEXT_PLAIN = 'text/plain'


class IncompleteFrame(Exception):
    """The frame has incomplete body"""


class BodyNotTerminated(Exception):
    """The frame's body is not terminated with the NULL character"""


class EmptyBuffer(Exception):
    """The buffer is empty"""


def parse_headers(buff):
    """
    Parses buffer and returns command and headers as strings

    @param buff: Buffer containing frame
    @type buff: C{io.BytesIO}

    """

    preamble_lines = list(map(
        lambda x: x.decode(),
        iter(lambda: buff.readline().strip(), b''))
    )
    if not preamble_lines:
        raise EmptyBuffer()
    return preamble_lines[0], OrderedDict([l.split(':') for l in preamble_lines[1:]])


def parse_body(buff, headers):
    """

    @param buff: Buffer containing frame
    @type buff: C{io.BytesIO}

    @param headers: Dictionary of headers
    @type headers: C{dict}

    """
    content_length = int(headers.get('content-length', -1))
    body = buff.read(content_length)
    if content_length >= 0:
        if len(body) < content_length:
            raise IncompleteFrame()
        terminator = buff.read(1).decode()
        if not terminator:
            raise BodyNotTerminated()
    else:
        # no content length
        body, terminator, rest = body.partition(b'\x00')
        if not terminator:
            raise BodyNotTerminated()
        else:
            buff.seek(-len(rest), 2)

    return body


class Frame:
    """
    A STOMP frame (or message).

    :param cmd: the protocol command
    :param headers: a map of headers for the frame
    :param body: the content of the frame.
    """

    def __init__(self, cmd, headers=None, body=None):
        self.cmd = cmd
        self.headers = headers or {}
        self.body = body or ''

    def __str__(self):
        return f'{{cmd={self.cmd},headers=[{self.headers}],body={self.body if isinstance(self.body, bytes) else self.body.encode()}}}'

    def __eq__(self, other):
        """ Override equality checking to test for matching command, headers, and body. """
        return all([isinstance(other, Frame),
                    self.cmd == other.cmd,
                    self.headers == other.headers,
                    self.body == other.body])

    @property
    def transaction(self):
        return self.headers.get('transaction')

    @classmethod
    def from_buffer(cls, buff):
        cmd, headers = parse_headers(buff)
        body = parse_body(buff, headers)
        return cls(cmd, headers=headers, body=body)

    def pack(self):
        """
        Create a string representation from object state.

        @return: The string (bytes) for this stomp frame.
        @rtype: C{str}
        """

        self.headers.setdefault('content-length', len(self.body))

        # See https://stomp.github.io/stomp-specification-1.1.html#Augmented_BNF
        return (
            # command LF
            self.cmd.encode() + b"\n" +
            # *( header LF )
            "".join(starmap("{0}:{1}\n".format, self.headers.items())).encode() +
            # LF
            b"\n" +
            # *OCTET
            (self.body if isinstance(self.body, bytes) else self.body.encode()) +
            # NULL
            b'\x00'
        )


class ConnectedFrame(Frame):
    """ A CONNECTED server frame (response to CONNECT).

    @ivar session: The (throw-away) session ID to include in response.
    @type session: C{str}
    """

    def __init__(self, session, extra_headers=None):
        """
        @param session: The (throw-away) session ID to include in response.
        @type session: C{str}
        """
        super().__init__(
            cmd=CONNECTED, headers=extra_headers or {})
        self.headers['session'] = session


class HeaderValue:
    """
    An descriptor class that can be used when a calculated header value is needed.

    This class is a descriptor, implementing  __get__ to return the calculated value.
    While according to  U{https://docs.codehaus.org/display/STOMP/Character+Encoding} there
    seems to some general idea about having UTF-8 as the character encoding for headers;
    however the C{stomper} lib does not support this currently.

    For example, to use this class to generate the content-length header:

        >>> body = 'asdf'
        >>> headers = {}
        >>> headers['content-length'] = HeaderValue(calculator=lambda: len(body))
        >>> str(headers['content-length'])
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
            raise ValueError("Non-callable param: {calculator}")
        self.calc = calculator

    def __get__(self, obj, objtype):
        return self.calc()

    def __str__(self):
        return str(self.calc())

    def __set__(self, obj, value):
        self.calc = value

    def __repr__(self):
        return f'<{self.__class__.__name__} calculator={self.calc}>'


class ErrorFrame(Frame):
    """ An ERROR server frame. """

    def __init__(self, message, body=None, extra_headers=None):
        """
        @param body: The message body bytes.
        @type body: C{str}
        """
        super().__init__(cmd=ERROR,
                                         headers=extra_headers or {}, body=body)
        self.headers['message'] = message
        self.headers[
            'content-length'] = HeaderValue(calculator=lambda: len(self.body))

    def __repr__(self):
        return f'<{self.__class__.__name__} message={self.headers["message"]!r}>'


class ReceiptFrame(Frame):
    """ A RECEIPT server frame. """

    def __init__(self, receipt, extra_headers=None):
        """
        @param receipt: The receipt message ID.
        @type receipt: C{str}
        """
        super().__init__(
            'RECEIPT', headers=extra_headers or {})
        self.headers['receipt-id'] = receipt


class FrameBuffer:
    """
    A customized version of the StompBuffer class from Stomper project that returns frame objects
    and supports iteration.

    This version of the parser also assumes that stomp messages with no content-length
    end in a simple \\x00 char, not \\x00\\n as is assumed by
    C{stomper.stompbuffer.StompBuffer}. Additionally, this class differs from Stomper version
    by conforming to PEP-8 coding style.

    This class can be used to smooth over a transport that may provide partial frames (or
    may provide multiple frames in one data buffer).

    @ivar _buffer: The internal byte buffer.
    @type _buffer: C{io.BytesIO}

    @ivar debug: Log extra parsing debug (logs will be DEBUG level).
    @type debug: C{bool}
    """

    # regexp to check that the buffer starts with a command.
    command_re = re.compile('^(.+?)\n')

    # regexp to remove everything up to and including the first
    # instance of '\x00' (used in resyncing the buffer).
    sync_re = re.compile('^.*?\x00')

    # regexp to determine the content length. The buffer should always start
    # with a command followed by the headers, so the content-length header will
    # always be preceded by a newline.  It may not always proceeded by a
    # newline, though!
    content_length_re = re.compile('\ncontent-length\\s*:\\s*(\\d+)\\s*(\n|$)')

    def __init__(self):
        self._buffer = io.BytesIO()
        self._pointer = 0
        self.debug = False
        self.log = logging.getLogger(f'{self.__module__}.{self.__class__.__name__}')

    def clear(self):
        """
        Clears (empties) the internal buffer.
        """
        self._buffer = io.BytesIO()

    def buffer_len(self):
        """
        @return: Number of bytes in the internal buffer.
        @rtype: C{int}
        """
        return self._buffer.getbuffer().nbytes

    def buffer_empty(self):
        """
        @return: C{True} if buffer is empty, C{False} otherwise.
        @rtype: C{bool}
        """
        return self._buffer.getbuffer().nbytes > 0

    def append(self, data):
        """
        Appends bytes to the internal buffer (may or may not contain full stomp frames).

        @param data: The bytes to append.
        @type data: C{bytes}
        """
        self._buffer.write(data)

    def extract_frame(self):
        """
        Pulls one complete frame off the buffer and returns it.

        If there is no complete message in the buffer, returns None.

        Note that the buffer can contain more than once message. You
        should therefore call this method in a loop (or use iterator
        functionality exposed by class) until None returned.

        @return: The next complete frame in the buffer.
        @rtype: L{stomp.frame.Frame}
        """
        self._buffer.seek(self._pointer, 0)
        try:
            f = Frame.from_buffer(self._buffer)
            self._pointer = self._buffer.tell()
        except (IncompleteFrame, EmptyBuffer):
            # Seek to the end of the buffer so the next call to
            # `append()` does not overwrite part of the frame.
            self._buffer.seek(0, 2)
            return None

        return f

    def __iter__(self):
        """
        Returns an iterator object.
        """
        return self

    def __next__(self):
        """
        Return the next STOMP message in the buffer (supporting iteration).

        @rtype: L{stomp.frame.Frame}
        """
        msg = self.extract_frame()
        if not msg:
            raise StopIteration()
        return msg
