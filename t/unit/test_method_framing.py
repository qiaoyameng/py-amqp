import threading
from struct import pack
from unittest.mock import Mock

import pytest

from amqp import spec
from amqp.basic_message import Message
from amqp.exceptions import UnexpectedFrame
from amqp.method_framing import frame_handler, frame_writer


class test_frame_handler:

    @pytest.fixture(autouse=True)
    def setup_conn(self):
        self.conn = Mock(name='connection')
        self.conn.bytes_recv = 0
        self.callback = Mock(name='callback')
        self.g = frame_handler(self.conn, self.callback)

    def test_header(self):
        buf = pack('>HH', 60, 51)
        assert self.g((1, 1, buf))
        self.callback.assert_called_with(1, (60, 51), buf, None)
        assert self.conn.bytes_recv

    def test_header_message_empty_body(self):
        assert not self.g((1, 1, pack('>HH', *spec.Basic.Deliver)))
        self.callback.assert_not_called()

        with pytest.raises(UnexpectedFrame):
            self.g((1, 1, pack('>HH', *spec.Basic.Deliver)))

        m = Message()
        m.properties = {}
        buf = pack('>HxxQ', m.CLASS_ID, 0)
        buf += m._serialize_properties()
        assert self.g((2, 1, buf))

        self.callback.assert_called()
        msg = self.callback.call_args[0][3]
        self.callback.assert_called_with(
            1, msg.frame_method, msg.frame_args, msg,
        )

    def test_header_message_content(self):
        assert not self.g((1, 1, pack('>HH', *spec.Basic.Deliver)))
        self.callback.assert_not_called()

        m = Message()
        m.properties = {}
        buf = pack('>HxxQ', m.CLASS_ID, 16)
        buf += m._serialize_properties()
        assert not self.g((2, 1, buf))
        self.callback.assert_not_called()

        assert not self.g((3, 1, b'thequick'))
        self.callback.assert_not_called()

        assert self.g((3, 1, b'brownfox'))
        self.callback.assert_called()
        msg = self.callback.call_args[0][3]
        self.callback.assert_called_with(
            1, msg.frame_method, msg.frame_args, msg,
        )
        assert msg.body == b'thequickbrownfox'

    def test_heartbeat_frame(self):
        assert not self.g((8, 1, ''))
        self.callback.assert_not_called()
        assert self.conn.bytes_recv


class test_frame_writer:

    @pytest.fixture(autouse=True)
    def setup_conn(self):
        self.connection = Mock(name='connection')
        self.transport = self.connection.Transport()
        self.connection.frame_max = 512
        self.connection.bytes_sent = 0
        self.g = frame_writer(self.connection, self.transport)
        self.write = self.transport.write

    def test_write_fast_header(self):
        frame = 1, 1, spec.Queue.Declare, b'x' * 30, None
        self.g(*frame)
        self.write.assert_called()

    def test_write_fast_content(self):
        msg = Message(body=b'y' * 10, content_type='utf-8')
        frame = 2, 1, spec.Basic.Publish, b'x' * 10, msg
        self.g(*frame)
        self.write.assert_called()
        assert 'content_encoding' not in msg.properties

    def test_write_slow_content(self):
        msg = Message(body=b'y' * 2048, content_type='utf-8')
        frame = 2, 1, spec.Basic.Publish, b'x' * 10, msg
        self.g(*frame)
        self.write.assert_called()
        assert 'content_encoding' not in msg.properties

    def test_write_zero_len_body(self):
        msg = Message(body=b'', content_type='application/octet-stream')
        frame = 2, 1, spec.Basic.Publish, b'x' * 10, msg
        self.g(*frame)
        self.write.assert_called()
        assert 'content_encoding' not in msg.properties

    def test_write_fast_unicode(self):
        msg = Message(body='\N{CHECK MARK}')
        frame = 2, 1, spec.Basic.Publish, b'x' * 10, msg
        self.g(*frame)
        self.write.assert_called()
        memory = self.write.call_args[0][0]
        assert isinstance(memory, memoryview)
        assert '\N{CHECK MARK}'.encode() in memory.tobytes()
        assert msg.properties['content_encoding'] == 'utf-8'

    def test_write_slow_unicode(self):
        msg = Message(body='y' * 2048 + '\N{CHECK MARK}')
        frame = 2, 1, spec.Basic.Publish, b'x' * 10, msg
        self.g(*frame)
        self.write.assert_called()
        memory = self.write.call_args[0][0]
        assert isinstance(memory, bytes)
        assert '\N{CHECK MARK}'.encode() in memory
        assert msg.properties['content_encoding'] == 'utf-8'

    def test_write_non_utf8(self):
        msg = Message(body='body', content_encoding='utf-16')
        frame = 2, 1, spec.Basic.Publish, b'x' * 10, msg
        self.g(*frame)
        self.write.assert_called()
        memory = self.write.call_args[0][0]
        assert isinstance(memory, memoryview)
        assert 'body'.encode('utf-16') in memory.tobytes()
        assert msg.properties['content_encoding'] == 'utf-16'

    def test_write_frame__fast__buffer_store_resize(self):
        """The buffer_store is resized when the connection's frame_max is increased."""
        small_msg = Message(body='t')
        small_frame = 2, 1, spec.Basic.Publish, b'x' * 10, small_msg
        self.g(*small_frame)
        self.write.assert_called_once()
        write_arg = self.write.call_args[0][0]
        assert isinstance(write_arg, memoryview)
        assert len(write_arg) < self.connection.frame_max
        self.connection.reset_mock()

        # write a larger message to the same frame_writer after increasing frame_max
        large_msg = Message(body='t' * (self.connection.frame_max + 10))
        large_frame = 2, 1, spec.Basic.Publish, b'x' * 10, large_msg
        original_frame_max = self.connection.frame_max
        self.connection.frame_max += 100
        self.g(*large_frame)
        self.write.assert_called_once()
        write_arg = self.write.call_args[0][0]
        assert isinstance(write_arg, memoryview)
        assert len(write_arg) > original_frame_max

    def test_write_frame__concurrent_writers_are_serialized(self):
        """A second thread must not touch the shared buffer while a write
        is in flight (#462)."""
        in_write = threading.Event()
        release_write = threading.Event()
        seen = []

        def blocking_write(view):
            # Snapshot what the socket would send *after* it has consumed
            # the view; another writer must not be able to change it.
            in_write.set()
            release_write.wait(2)
            seen.append(bytes(view))

        self.transport.write.side_effect = blocking_write
        frame_a = 2, 1, spec.Basic.Publish, b'x' * 10, Message(body=b'A' * 40)
        frame_b = 2, 1, spec.Basic.Publish, b'x' * 10, Message(body=b'B' * 40)

        writer_a = threading.Thread(target=self.g, args=frame_a)
        writer_a.start()
        assert in_write.wait(2)

        b_started = threading.Event()

        def write_b():
            b_started.set()
            self.g(*frame_b)

        writer_b = threading.Thread(target=write_b)
        writer_b.start()
        try:
            # Only check once B is actually running, so a slow scheduler
            # can't make this pass on an unlocked implementation.
            assert b_started.wait(2)
            writer_b.join(0.2)
            # B must not reach the transport while A's write is in flight.
            assert writer_b.is_alive()
            assert self.transport.write.call_count == 1
        finally:
            release_write.set()
            writer_a.join(2)
            writer_b.join(2)
        assert not writer_a.is_alive() and not writer_b.is_alive()
        assert self.transport.write.call_count == 2
        assert b'A' * 40 in seen[0] and b'B' * 40 not in seen[0]
        assert b'B' * 40 in seen[1] and b'A' * 40 not in seen[1]


def encode_frames(frame_max, channel, method_sig, args, content):
    """Encode a method with content into real wire frames."""
    from struct import unpack_from

    conn = Mock(name='connection')
    conn.frame_max = frame_max
    conn.bytes_sent = 0
    transport = Mock(name='transport')
    written = []
    transport.write = lambda data: written.append(bytes(data))
    writer = frame_writer(conn, transport)
    writer(1, channel, method_sig, args, content)

    frames = []
    buf = b''.join(written)
    offset = 0
    while offset < len(buf):
        ftype, fchannel, size = unpack_from('>BHI', buf, offset)
        payload = buf[offset + 7:offset + 7 + size]
        assert buf[offset + 7 + size] == 0xce
        frames.append((ftype, fchannel, payload))
        offset += 8 + size
    return frames


class test_frame_handler_body_sink:

    @pytest.fixture(autouse=True)
    def setup_conn(self):
        from amqp.body_sink import spooled_body_sink

        self.conn = Mock(name='connection')
        self.conn.bytes_recv = 0
        self.callback = Mock(name='callback')
        self.sinks = {}
        spool = spooled_body_sink(max_size=16)

        def tracking_factory(body_size, channel):
            sink = spool(body_size, channel)
            self.sinks[channel] = sink
            return sink

        self.g = frame_handler(
            self.conn, self.callback, body_sink_factory=tracking_factory)

    def test_small_message_keeps_bytes_body(self):
        frames = encode_frames(
            512, 1, spec.Basic.Deliver, b'x' * 10,
            Message(body=b'thequickbrownfox'))
        for frame in frames[:-1]:
            assert not self.g(frame)
        assert self.g(frames[-1])
        msg = self.callback.call_args[0][3]
        assert type(msg.body) is bytes
        assert msg.body == b'thequickbrownfox'

    def test_large_message_spooled_to_file(self):
        from amqp.body_sink import FileBody

        body = b'y' * 40
        frames = encode_frames(
            24, 1, spec.Basic.Deliver, b'x' * 10, Message(body=body))
        for frame in frames[:-1]:
            assert not self.g(frame)
        assert self.g(frames[-1])
        msg = self.callback.call_args[0][3]
        assert isinstance(msg.body, FileBody)
        try:
            assert msg.body.read() == body
        finally:
            msg.body.close()
        assert msg.body.closed

    def test_interleaved_messages_channel_close(self):
        """Two interleaved messages; closing one channel mid-assembly
        must release its half-finished body without affecting the
        other channel's message."""
        from amqp.body_sink import FileBody

        body_a = b'A' * 40
        body_b = b'B' * 40
        frames_a = encode_frames(
            24, 1, spec.Basic.Deliver, b'args-a', Message(body=body_a))
        frames_b = encode_frames(
            24, 2, spec.Basic.Deliver, b'args-b', Message(body=body_b))
        # method + header + 3 body frames each
        assert [f[0] for f in frames_a] == [1, 2, 3, 3, 3]
        assert [f[0] for f in frames_b] == [1, 2, 3, 3, 3]

        # interleave both messages up to (not including) their last frames
        for frame_a, frame_b in zip(frames_a[:-1], frames_b[:-1]):
            assert not self.g(frame_a)
            assert not self.g(frame_b)
        self.callback.assert_not_called()

        # each channel has its own sink
        sink_a = self.g.partial_messages[1].body_sink
        sink_b = self.g.partial_messages[2].body_sink
        assert sink_a is not sink_b
        assert sink_a is self.sinks[1]
        assert sink_b is self.sinks[2]
        file_b = sink_b._file
        assert not file_b.closed

        # close channel 2 before its last frame arrives
        self.g.discard(2)
        assert 2 not in self.g.partial_messages
        assert file_b.closed
        assert sink_b._file is None

        # the other message is still assembled correctly
        assert self.g(frames_a[-1])
        self.callback.assert_called_once()
        channel, method_sig, _, msg = self.callback.call_args[0]
        assert channel == 1
        assert method_sig == spec.Basic.Deliver
        assert isinstance(msg.body, FileBody)
        with msg.body:
            assert msg.body.read() == body_a

    def test_body_frame_exceeding_declared_size(self):
        """A body frame must not exceed the length declared by the
        content header; the half-finished content is released."""
        frames = encode_frames(
            512, 1, spec.Basic.Deliver, b'x' * 10, Message(body=b'ab'))
        assert not self.g(frames[0])  # method
        assert not self.g(frames[1])  # header declares 2 bytes
        sink = self.g.partial_messages[1].body_sink
        with pytest.raises(UnexpectedFrame):
            self.g((3, 1, b'thequickbrownfox'))
        self.callback.assert_not_called()
        assert 1 not in self.g.partial_messages
        assert sink._file is None

    def test_close_releases_all_partial_messages(self):
        frames_a = encode_frames(
            24, 1, spec.Basic.Deliver, b'args-a', Message(body=b'A' * 40))
        frames_b = encode_frames(
            24, 2, spec.Basic.Deliver, b'args-b', Message(body=b'B' * 40))
        for frame in frames_a[:-1] + frames_b[:-1]:
            self.g(frame)
        file_a = self.g.partial_messages[1].body_sink._file
        file_b = self.g.partial_messages[2].body_sink._file

        self.g.close()

        assert not self.g.partial_messages
        assert file_a.closed
        assert file_b.closed
        self.callback.assert_not_called()
