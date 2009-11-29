"""
Functional tests to test full stack -- i.e. reactor, etc.
"""
from twisted.internet.address import IPv4Address
from twisted.test import proto_helpers

class TestCaseStompClient(object):
    
    def __init__(self, factory, address=None):
        if address is None: address = IPv4Address('TCP', '127.0.0.1', 61613)
        self.transport = proto_helpers.StringTransportWithDisconnection()
        self.protocol = factory.buildProtocol(address)
        self.transport.protocol = self.protocol
        self.protocol.makeConnection(self.transport)

    def write(self, stuff):
        if isinstance(stuff, unicode):
            stuff = stuff.encode('utf-8')
        self.protocol.dataReceived(stuff)