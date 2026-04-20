"""Test the FrameBuffer utility class."""

import io
import uuid
from collections import OrderedDict

import pytest

from coilmq.util import frames
from coilmq.util.frames import (
    BodyNotTerminated,
    Frame,
    FrameBuffer,
    IncompleteFrame,
    parse_body,
    parse_headers,
)

__authors__ = ['"Hans Lellelid" <hans@xmpl.org>']
__copyright__ = "Copyright 2010 Hans Lellelid"
__license__ = """Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""


class TestFrameBuffer:
    """Test the :class:`coilmq.utils.frames.FrameBuffer` class."""

    def createMessage(self, cmd, headers, body):
        """Creates a package STOMP message."""
        return Frame(cmd, headers=headers, body=body).pack()

    def test_extract_frame(self):
        """Test extracting a single frame."""
        sb = FrameBuffer()
        m1 = self.createMessage(
            frames.CONNECT, {"session": uuid.uuid4()}, "This is the body"
        )
        sb.append(m1)
        msg = sb.extract_frame()
        assert isinstance(msg, Frame)
        assert m1 == msg.pack()

    def test_extract_frame_binary(self):
        """Test extracting a binary frame."""
        sb = FrameBuffer()
        binmsg = "\x00\x00HELLO\x00\x00DONKEY\x00\x00"
        m1 = self.createMessage(
            frames.SEND,
            OrderedDict({"content-length": len(binmsg), "x-other-header": "value"}),
            binmsg,
        )
        sb.append(m1)
        msg = sb.extract_frame()
        assert isinstance(msg, Frame)
        assert msg.pack() == m1

    def test_extract_frame_multi(self):
        """Test the handling of multiple concatenated messages by the buffer."""
        m1 = b"CONNECT\nsession:207567f3-cce7-4a0a-930b-46fc394dd53d\n\n0123456789\x00"
        m2 = b"SUBSCRIBE\nack:auto\ndestination:/queue/test\n\n\x00SEND\ndestination:/queue/test\n\n\x00"

        sb = FrameBuffer()
        sb.append(m1)
        f1 = sb.extract_frame()
        assert f1.cmd == frames.CONNECT
        assert f1.body == b"0123456789"

        f = sb.extract_frame()

        assert f is None

        sb.append(m2)
        f2 = sb.extract_frame()
        f3 = sb.extract_frame()

        assert f2.cmd == frames.SUBSCRIBE
        assert f2.body == ""
        assert f3.cmd == frames.SEND
        assert f3.headers.get("destination") == "/queue/test"
        assert f3.body == ""

        assert sb.extract_frame() is None

    def test_iteration(self):
        """Test the iteration feature of our buffer."""
        sb = FrameBuffer()
        m1 = self.createMessage(
            frames.CONNECT, {"session": uuid.uuid4()}, b"This is the body"
        )
        m2 = self.createMessage(
            frames.SEND, {"destination": "/queue/sample"}, b"This is the body-2"
        )
        sb.append(m1)
        sb.append(m2)

        assert sb is iter(sb)

        idx = 0
        expected = (m1, m2)
        for frame in sb:
            assert isinstance(frame, Frame)
            assert expected[idx] == frame.pack()
            idx += 1

        assert idx == 2


class TestFrame:
    def test_parse_frame(self):
        buff = io.BytesIO(
            b"CONNECT\nsession:207567f3-cce7-4a0a-930b-46fc394dd53d\n\n0123456789\x00"
        )
        cmd, headers = parse_headers(buff)
        parse_body(buff, headers)

        assert isinstance(cmd, str)
        assert cmd == frames.CONNECT
        assert headers["session"] == "207567f3-cce7-4a0a-930b-46fc394dd53d"

        for e in [cmd] + list(headers.keys()) + list(headers.values()):
            assert isinstance(e, str)

    def test_parse_frame_incomplete_body(self):
        buff = io.BytesIO(b"CONNECT\ncontent-length:1000\n\n0123456789\x00")
        with pytest.raises(IncompleteFrame):
            Frame.from_buffer(buff)

    def test_parse_frame_body_not_terminated(self):
        buff = io.BytesIO(b"CONNECT\ncontent-length:10\n\n0123456789")
        with pytest.raises(BodyNotTerminated):
            Frame.from_buffer(buff)

    def test_parse_frame_empty_body(self):
        buff = io.BytesIO(
            b"SUBSCRIBE\nack:auto\ndestination:/queue/test\n\n\x00fdffdfd"
        )
        Frame.from_buffer(buff)

    def test_pack(self):
        frame = Frame(frames.CONNECT, OrderedDict(foo="bar"), "body")
        assert frame.pack() == b"CONNECT\nfoo:bar\ncontent-length:4\n\nbody\x00"

    def test_pack_binary(self):
        bin_body = "\x00\x00HELLO\x00\x00DONKEY\x00\x00"
        frame = Frame(frames.CONNECT, body=bin_body)
        assert (
            frame.pack()
            == b"CONNECT\ncontent-length:17\n\n\x00\x00HELLO\x00\x00DONKEY\x00\x00\x00"
        )
