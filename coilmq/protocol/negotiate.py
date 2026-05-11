from __future__ import annotations

from typing import TYPE_CHECKING, Final

from coilmq.exception import ProtocolError
from coilmq.protocol.adaptor import STOMP10, STOMP11, STOMP12
from coilmq.protocol.version import Version
from coilmq.util.frames import CONNECT, STOMP_CMD

if TYPE_CHECKING:
    from coilmq.protocol.adaptor import STOMP
    from coilmq.util.frames import Frame

SUPPORTED_VERSIONS: Final[dict[Version, type[STOMP]]] = {
    Version.from_str("1.0"): STOMP10,
    Version.from_str("1.1"): STOMP11,
    Version.from_str("1.2"): STOMP12,
}


def protocol_class_from_first_frame(frame: Frame) -> type[STOMP]:
    """Return the latest compatible protocol for a given ``CONNECT`` frame.

    STOMP requires a client to send a ``CONNECT`` frame after the socket
    connection is established.  (This function also recognizes the ``STOMP``
    command, introduced in STOMP 1.1, as an alias for the ``CONNECT`` command.)
    This function inspects the contents of the first frame and returns an
    instance of a :class:`STOMP` concrete implementation that handles the latest
    protocol version supported by both the client and the server.

    :param frame: the first frame received from a new connection

    :returns: A :class:`STOMP` subclass whose version corresponds to the maximum
        common version accepted by the client.

    :raises: :class:`ProtocolError` if the frame's command is not one of
        ``CONNECT`` or ``STOMP``, if a ``STOMP`` frame is missing the ``accept-version``
        header, if the frame's ``accept-version`` header is malformed, or if the
        ``accept-version`` header does not contain any version in common with the
        versions supported by the server.

    """
    if frame.cmd in (CONNECT, STOMP_CMD) and "accept-version" in frame.headers:
        # Convert a value like ``"1.0,1.1,1.2"`` into
        # ``{Version(1, 0), Version(1, 1), Version(1, 2)}``.
        try:
            client_versions = {
                Version.from_str(v)
                for v in set(frame.headers["accept-version"].split(","))
            }
        except ValueError as err:
            msg = "Malformed 'accept-version'"
            raise ProtocolError(msg) from err

        common = client_versions & SUPPORTED_VERSIONS.keys()

        if not common:
            server_versions = " ".join(
                str(v) for v in sorted(SUPPORTED_VERSIONS.keys())
            )
            msg = f"Supported protocol versions are {server_versions}"
            raise ProtocolError(msg)

        version = max(common)

        protocol_class = SUPPORTED_VERSIONS[version]

    elif frame.cmd == CONNECT:
        # A ``CONNECT`` frame without ``accept-version`` is indicative of a
        # STOMP 1.0 client.
        protocol_class = STOMP10

    elif frame.cmd == STOMP_CMD:
        # The ``STOMP`` command, on the other hand, requires the
        # ``accept-version`` header.
        msg = "No 'accept-version' specified for STOMP command"
        raise ProtocolError(msg)

    else:
        # A client's first frame must be a ``CONNECT`` or ``STOMP`` command.
        msg = "Expected CONNECT or STOMP command"
        raise ProtocolError(msg)

    return protocol_class
