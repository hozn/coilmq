"""
Test the StompFrameBuffer utility class.
"""
import unittest
import uuid

import stomper

from coilmq.util.buffer import StompFrameBuffer

class TestStompFrameBuffer(unittest.TestCase):
    """
    Test the L{coilmq.utils.buffer.StompFrameBuffer} class.
    """
    
    def setUp(self):
        pass
    
    def tearDown(self):
        pass
    
    def createMessage(self, cmd, headers, body):
        """ Creates a package STOMP message. """
        frame = stomper.Frame()
        frame.setCmd(cmd)
        frame.headers = headers
        frame.body = body
        return frame.pack()
    
    def test_getOneMessage(self):
        """ Make sure we get a Frame back from getNextMessage() """
        sb = StompFrameBuffer()
        m1 = self.createMessage('connect', {'session': uuid.uuid4()}, 'This is the body')
        sb.appendData(m1)
        msg = sb.getOneMessage()
        assert isinstance(msg, stomper.Frame)
        assert m1 == msg.pack()
    
    def test_getOneMessage_multi(self):
        """ Test the handling of multiple concatenated messages by the buffer. """
        
        m1 = 'CONNECT\nsession:207567f3-cce7-4a0a-930b-46fc394dd53d\n\n0123456789\x00\n'
        m2 = 'SUBSCRIBE\nack:auto\ndestination:/queue/test\n\n\x00SEND\ndestination:/queue/test\n\n\x00'
        
        sb = StompFrameBuffer()
        sb.appendData(m1)
        f1 = sb.getOneMessage()
        assert f1.cmd == 'CONNECT'
        assert f1.body == '0123456789'
        
        assert sb.getOneMessage() is None
        
        sb.appendData(m2)
        f2 = sb.getOneMessage()
        f3 = sb.getOneMessage()
        
        assert f2.cmd == 'SUBSCRIBE'
        assert f2.body == ''
        assert f3.cmd == 'SEND'
        assert f3.destination == '/queue/test'
        assert f3.body == ''
        
        assert sb.getOneMessage() is None
        
        
    def test_getOneMessage_fragmented(self):
        """ Test the handling of fragmented frame data. """
        
        m1_1  = 'CONNECT\nsession:207567f3-cce7-4a0a-930b-'
        m1_2 = '46fc394dd53d\n\n0123456789\x00\nSUBSCRIBE\nack:a'
        
        m1_3 = 'uto\ndestination:/queue/test\n\n\x00SE'
        m1_4 = 'ND\ndestination:/queue/test\n\n0123456789\x00'
        
        sb = StompFrameBuffer()
        sb.appendData(m1_1)
        
        assert sb.getOneMessage() is None
        
        sb.appendData(m1_2)
        
        f1 = sb.getOneMessage()
        assert f1.cmd == 'CONNECT'
        assert f1.body == '0123456789'
        assert sb.getOneMessage() is None
        
        sb.appendData(m1_3)
        f2 = sb.getOneMessage()
        assert f2.cmd == 'SUBSCRIBE'        
        assert sb.getOneMessage() is None
        
        sb.appendData(m1_4)
        f3 = sb.getOneMessage()
        assert f3.cmd == 'SEND'
        assert f3.destination == '/queue/test'
        assert f3.body == '0123456789'
        
    def test_iteration(self):
        """ Test the iteration feature of our buffer."""
        sb = StompFrameBuffer()
        m1 = self.createMessage('connect', {'session': uuid.uuid4()}, 'This is the body')
        m2 = self.createMessage('send', {'destination': '/queue/sample'}, 'This is the body-2')
        print '%r' % m1
        print '%r' % m2
        sb.appendData(m1)
        sb.appendData(m2)
        
        assert sb is iter(sb)
        
        idx = 0
        expected = (m1, m2)
        for frame in sb:
            assert isinstance(frame, stomper.Frame)
            assert expected[idx] == frame.pack()
            idx += 1
        
        assert idx == 2
    
    