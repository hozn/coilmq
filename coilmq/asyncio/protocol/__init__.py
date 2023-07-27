import abc
import asyncio
import uuid
import socket
import datetime

from coilmq.exception import ProtocolError, AuthError
from coilmq.util import frames
from coilmq.util.frames import Frame, ErrorFrame, ReceiptFrame, ConnectedFrame
from coilmq.engine import StompEngine


class STOMP(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, engine: StompEngine):
        self.engine = engine

    def stomp(self, frame):
        self.connect(frame)

    @abc.abstractmethod
    async def process_frame(self, frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def connect(self, frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def send(self, frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def subscribe(self, frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def unsubscribe(self, frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def begin(self, frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def commit(self, frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def abort(self, frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def ack(self, frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def disconnect(self, frame):
        raise NotImplementedError

    @abc.abstractmethod
    async def process_heartbeat(self):
        raise NotImplementedError

    @abc.abstractmethod
    async def disable_heartbeat(self):
        raise NotImplementedError


class STOMP10(STOMP):

    def _check_required_header(self, frame: Frame, header: str) -> str:
        if header not in frame.headers:
            raise ProtocolError(f'`{header}` header is required for `{frame.cmd}` command')
        return frame.headers[header]

    def _validate_transaction(self, transaction):
        if transaction not in self.engine.transactions:
            raise ProtocolError(f"Invalid transaction: {transaction}")

    async def process_frame(self, frame: Frame):
        """
        Dispatches a received frame to the appropriate internal method.

        @param frame: The frame that was received.
        @type frame: C{stompclient.frame.Frame}
        """
        if frame.cmd not in frames.VALID_COMMANDS:
            raise ProtocolError(f"Invalid STOMP command: {frame.cmd}")

        method = getattr(self, frame.cmd.lower(), None)

        if not self.engine.connected and method not in (self.connect, self.stomp):
            raise ProtocolError("Not connected.")

        try:
            transaction = frame.headers.get('transaction')
            if not transaction or method in (self.begin, self.commit, self.abort):
                await method(frame)
            else:
                if transaction not in self.engine.transactions:
                    raise ProtocolError(f"Invalid transaction specified: {transaction}")
                self.engine.transactions[transaction].append(frame)
        except Exception as e:
            self.engine.log.error(f"Error processing STOMP frame: %s" % e)
            self.engine.log.exception(e)
            try:
                await self.engine.connection.send_frame(ErrorFrame(str(e), str(e)))
            except Exception as e:  # pragma: no cover
                self.engine.log.error("Could not send error frame: %s" % e)
                self.engine.log.exception(e)
        else:
            # The protocol is not especially clear here (not sure why I'm surprised)
            # about the expected behavior WRT receipts and errors.  We will assume that
            # the RECEIPT frame should not be sent if there is an error frame.
            # Also we'll assume that a transaction should not preclude sending the receipt
            # frame.
            # import pdb; pdb.set_trace()
            if frame.headers.get('receipt') and method != self.connect:
                await self.engine.connection.send_frame(ReceiptFrame(
                    receipt=frame.headers.get('receipt')))

    async def process_heartbeat(self):
        pass

    async def disable_heartbeat(self):
        pass

    async def connect(self, frame: Frame, response=None):
        """
        Handle CONNECT command: Establishes a new connection and checks auth (if applicable).
        """
        self.engine.log.debug("CONNECT")

        if self.engine.authenticator:
            login = frame.headers.get('login')
            passcode = frame.headers.get('passcode')
            if not self.engine.authenticator.authenticate(login, passcode):
                raise AuthError(f"Authentication failed for {login}")

        self.engine.connected = True

        response = response or Frame(frames.CONNECTED)
        response.headers['session'] = uuid.uuid4()

        # TODO: Do we want to do anything special to track sessions?
        # (Actually, I don't think the spec actually does anything with this at all.)
        await self.engine.connection.send_frame(response)

    async def send(self, frame: Frame):
        """
        Handle the SEND command: Delivers a message to a queue or topic (default).
        """
        dest = self._check_required_header(frame, 'destination')
        if dest.startswith('/queue/'):
            self.engine.queue_manager.send(frame)
        else:
            self.engine.topic_manager.send(frame)

    async def subscribe(self, frame: Frame):
        """
        Handle the SUBSCRIBE command: Adds this connection to destination.
        """
        ack = frame.headers.get('ack')
        reliable = ack and ack.lower() == 'client'

        self.engine.connection.reliable_subscriber = reliable

        dest = self._check_required_header(frame, 'destination')
        id = frame.headers.get('id')
        if dest.startswith('/queue/'):
            self.engine.queue_manager.subscribe(self.engine.connection, dest, id=id)
        else:
            await self.engine.topic_manager.subscribe(self.engine.connection, dest, id=id)

    async def unsubscribe(self, frame: Frame):
        """
        Handle the UNSUBSCRIBE command: Removes this connection from destination.
        """
        dest = frame.headers.get('destination')
        id = frame.headers.get('id')
        if not dest and not id:
            raise ProtocolError(f'One of `destination` or `id` header are required for `{frame.cmd}` command')

        if dest.startswith('/queue/'):
            self.engine.queue_manager.unsubscribe(self.engine.connection, dest, id=id)
        else:
            await self.engine.topic_manager.unsubscribe(self.engine.connection, dest, id=id)

    async def begin(self, frame: Frame):
        """
        Handles BEGIN command: Starts a new transaction.
        """
        transaction = self._check_required_header(frame, 'transaction')
        self.engine.transactions[transaction] = []

    async def commit(self, frame: Frame):
        """
        Handles COMMIT command: Commits specified transaction.
        """
        transaction = self._check_required_header(frame, 'transaction')
        self._validate_transaction(transaction)

        for tframe in self.engine.transactions[transaction]:
            del tframe.headers['transaction']
            await self.process_frame(tframe)

        self.engine.queue_manager.clear_transaction_frames(
            self.engine.connection,
            transaction,
        )
        del self.engine.transactions[transaction]

    async def abort(self, frame: Frame):
        """
        Handles ABORT command: Rolls back specified transaction.
        """
        transaction = self._check_required_header(frame, 'transaction')
        self._validate_transaction(transaction)

        self.engine.queue_manager.resend_transaction_frames(
            self.engine.connection,
            transaction,
        )
        del self.engine.transactions[transaction]

    async def ack(self, frame: Frame):
        """
        Handles the ACK command: Acknowledges receipt of a message.
        """
        self._check_required_header(frame, 'message-id')
        self.engine.queue_manager.ack(self.engine.connection, frame, id=frame.headers.get("id"))

    async def disconnect(self, frame: Frame):
        """
        Handles the DISCONNECT command: Unbinds the connection.

        Clients are supposed to send this command, but in practice it should not be
        relied upon.
        """
        self.engine.log.debug("Disconnect")
        await self.engine.unbind()


class STOMP11(STOMP10):

    SUPPORTED_VERSIONS = {'1.0', '1.1'}

    def __init__(
            self,
            engine: StompEngine,
            send_heartbeat_interval: int = 100,
            receive_heartbeat_interval: int = 100,
            *args, **kwargs
    ):
        super(STOMP11, self).__init__(engine)
        self.last_hb = datetime.datetime.now()
        self.last_hb_sent = datetime.datetime.now()

        # flags to control heartbeating
        self.send_hb = self.receive_hb = False

        self.send_heartbeat_interval = datetime.timedelta(milliseconds=send_heartbeat_interval)
        self.receive_heartbeat_interval = datetime.timedelta(milliseconds=receive_heartbeat_interval)

        self.send_heartbeat_task = None
        self.receive_heartbeat_task = None


    async def enable_heartbeat(self, cx: int, cy: int, response: Frame):
        async def repeat_at_interval(coro, interval_secs):
            while True:
                await coro()
                await asyncio.sleep(interval_secs)


        if self.send_heartbeat_interval and cy:
            self.send_heartbeat_interval = max(self.send_heartbeat_interval, datetime.timedelta(milliseconds=cy))
            self.send_heartbeat_task = asyncio.create_task(
                repeat_at_interval(self.send_heartbeat, self.send_heartbeat_interval.total_seconds())
            )

        if self.receive_heartbeat_interval and cx:
            self.receive_heartbeat_interval = max(self.send_heartbeat_interval, datetime.timedelta(milliseconds=cx))
            self.receive_heartbeat_task = asyncio.create_task(
                repeat_at_interval(self.check_receive_heartbeat, self.receive_heartbeat_interval.total_seconds())
            )
        response.headers['heart-beat'] = '{0},{1}'.format(
            int(self.send_heartbeat_interval / datetime.timedelta(milliseconds=1)),
            int(self.receive_heartbeat_interval / datetime.timedelta(milliseconds=1)),
        )

    async def disable_heartbeat(self):
        if self.receive_heartbeat_task:
            self.receive_heartbeat_task.cancel()
        if self.receive_heartbeat_task:
            self.receive_heartbeat_task.cancel()

    async def send_heartbeat(self):
        if not self.engine.connected:
            return
        await self.engine.connection.send_heartbeat()
        self.last_hb_sent = datetime.datetime.now()

    async def check_receive_heartbeat(self):
        ago = datetime.datetime.now() - self.last_hb
        if ago > (self.receive_heartbeat_interval * 2):
            self.engine.log.debug(f"No heartbeat was received for {ago.total_seconds()} seconds")
            await self.engine.unbind()

    async def process_heartbeat(self):
        self.last_hb = datetime.datetime.now()

    async def connect(self, frame: Frame, response: Frame = None):
        connected_frame = Frame(frames.CONNECTED)
        await self._negotiate_protocol(frame, connected_frame)
        heart_beat = frame.headers.get('heart-beat', '0,0')
        if heart_beat:
            await self.enable_heartbeat(*map(int, heart_beat.split(',')), response=connected_frame)
        await super(STOMP11, self).connect(frame, response=connected_frame)

    async def nack(self, frame: Frame):
        """
        Handles the NACK command: Unacknowledges receipt of a message.
        For now, this is just a placeholder to implement this version of the protocol
        """
        self._check_required_header(frame, 'message-id')
        self._check_required_header(frame, 'subscription')

        raise NotImplementedError('Nack implementation incomplete')
        # ToDo: await self.engine.queue_manager.nack()

    async def _negotiate_protocol(self, frame: Frame, response: Frame):
        client_versions = frame.headers.get('accept-version', '1.0')
        if not client_versions:
            raise ProtocolError('No protocol accept-version specified')
        common = set(client_versions.split(',')) & self.SUPPORTED_VERSIONS
        if not common:
            versions = ','.join(self.SUPPORTED_VERSIONS)
            await self.engine.connection.send_frame(Frame(
                    frames.ERROR,
                    headers={'version': versions, 'content-type': frames.TEXT_PLAIN},
                    body=f'Supported protocol versions are {versions}'
            ))
        else:
            response.headers['version'] = max(common)
            protocol_class = PROTOCOL_MAP[response.headers['version']]
            if type(self) is not protocol_class:
                self.engine.protocol = protocol_class(self.engine)
                await self.engine.protocol.connect(frame, response=response)

    async def subscribe(self, frame: Frame):
        self._check_required_header(frame, 'id')
        await super().subscribe(frame)

    async def unsubscribe(self, frame: Frame):
        self._check_required_header(frame, 'id')
        await super().unsubscribe(frame)

    async def ack(self, frame: Frame):
        self._check_required_header(frame, 'message-id')
        subscription = self._check_required_header(frame, 'subscription')
        self.engine.queue_manager.ack(self.engine.connection, frame, id=subscription)


class STOMP12(STOMP11):

    SUPPORTED_VERSIONS = STOMP11.SUPPORTED_VERSIONS.union({'1.2', })

    async def connect(self, frame: Frame, response: Frame = None):
        host = self._check_required_header(frame, 'host')
        if host != socket.getfqdn():
            raise ProtocolError('Virtual hosting is not supported or host is unknown')
        await super(STOMP12, self).connect(frame, response)

    async def ack(self, frame: Frame):
        id = self._check_required_header(frame, 'id')
        self._check_required_header(frame, 'message-id')
        self.engine.queue_manager.ack(self.engine.connection, frame, id=id)


PROTOCOL_MAP = {'1.0': STOMP10, '1.1': STOMP11, '1.2': STOMP12}
