import threading
from struct import pack, unpack_from
from unittest.mock import Mock

import pytest

from amqp import Channel, Connection, spec
from amqp.basic_message import Message
from amqp.exceptions import FrameError, UnexpectedFrame
from amqp.method_framing import (
    FileBody, FileSink, MemorySink, SpooledBodySink, frame_handler,
    frame_writer,
)


class test_frame_handler:

    @pytest.fixture(autouse=True)
    def setup_conn(self):
        self.conn = Mock(name='connection')
        self.conn.bytes_recv = 0
        self.conn.body_sink = None
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


class test_body_sinks:

    def test_memory_sink_single_chunk(self):
        sink = MemorySink(3)
        sink.write(b'foo')
        assert sink.finish() == b'foo'

    def test_memory_sink_joins_chunks(self):
        sink = MemorySink(6)
        sink.write(b'foo')
        sink.write(b'bar')
        assert sink.finish() == b'foobar'

    def test_file_sink_finish_transfers_ownership(self):
        sink = FileSink(3)
        sink.write(b'foo')
        body = sink.finish()
        assert isinstance(body, FileBody)
        # closing the sink after delivery must not affect the body
        sink.close()
        assert not body.closed
        assert body.read() == b'foo'
        body.close()
        assert body.closed

    def test_file_sink_close_releases_file(self):
        sink = FileSink(3)
        sink.write(b'foo')
        fileobj = sink._file
        sink.close()
        assert fileobj.closed
        assert sink._file is None

    def test_file_body_read_and_close(self):
        sink = FileSink(6)
        sink.write(b'foobar')
        with sink.finish() as body:
            assert body.read(3) == b'foo'
            assert body.read() == b'bar'
        assert body.closed
        with pytest.raises(ValueError):
            body.read()

    def test_spooled_body_sink_threshold(self):
        factory = SpooledBodySink(threshold=10)
        assert isinstance(factory(10), MemorySink)
        assert isinstance(factory(11), FileSink)


def encode_frames(channel_id, msg, frame_max=64):
    """Encode a content method into real AMQP frames with frame_writer."""
    conn = Mock(name='write-connection')
    conn.frame_max = frame_max
    conn.bytes_sent = 0
    transport = Mock(name='transport')
    write = frame_writer(conn, transport)
    write(1, channel_id, spec.Basic.Deliver, b'x', msg)
    data = b''.join(
        bytes(c.args[0]) for c in transport.write.call_args_list)
    frames = []
    offset = 0
    while offset < len(data):
        frame_type, frame_channel, frame_len = unpack_from(
            '>BHI', data, offset)
        payload = data[offset + 7:offset + 7 + frame_len]
        assert data[offset + 7 + frame_len] == 0xce
        frames.append((frame_type, frame_channel, payload))
        offset += 8 + frame_len
    return frames


class test_frame_handler_body_sink:

    @pytest.fixture(autouse=True)
    def setup_conn(self):
        self.sinks = []
        spool = SpooledBodySink(threshold=16)

        def factory(body_size):
            sink = spool(body_size)
            self.sinks.append(sink)
            return sink

        self.conn = Connection(body_sink=factory)
        self.callback = Mock(name='callback')
        self.conn._on_inbound_frame = frame_handler(
            self.conn, self.callback)
        self.g = self.conn._on_inbound_frame

    def teardown_method(self):
        if self.conn.channels is not None:
            self.conn.collect()

    def test_small_message_stays_bytes(self):
        frames = encode_frames(1, Message(body=b'x' * 10))
        for frame in frames:
            self.g(frame)
        msg = self.callback.call_args[0][3]
        assert msg.body == b'x' * 10
        assert isinstance(msg.body, bytes)

    def test_large_message_spooled_to_file(self):
        frames = encode_frames(1, Message(body=b'x' * 100))
        for frame in frames:
            self.g(frame)
        msg = self.callback.call_args[0][3]
        assert isinstance(msg.body, FileBody)
        assert msg.body.read() == b'x' * 100
        msg.body.close()

    def test_interleaved_channels_and_channel_close(self):
        body1 = b'A' * 100
        body2 = b'B' * 120
        frames1 = encode_frames(1, Message(body=body1))
        frames2 = encode_frames(2, Message(body=body2))
        # both messages are split into several body frames
        assert [f[0] for f in frames1].count(3) > 1
        assert [f[0] for f in frames2].count(3) > 1

        ch1 = Channel(self.conn, 1)
        Channel(self.conn, 2)

        # interleave both messages, stopping before the last frame
        # of the first message
        self.g(frames1[0])
        self.g(frames2[0])
        self.g(frames1[1])
        self.g(frames2[1])
        for frame in frames1[2:-1]:
            self.g(frame)
        for frame in frames2[2:4]:
            self.g(frame)
        self.callback.assert_not_called()

        # each channel accumulated into its own sink
        assert len(self.sinks) == 2
        sink1, sink2 = self.sinks
        assert isinstance(sink1, FileSink)
        assert isinstance(sink2, FileSink)

        # closing channel 1 mid-message releases its partial content
        ch1.collect()
        assert sink1._file is None
        self.callback.assert_not_called()

        # the other message is still assembled correctly
        for frame in frames2[4:]:
            self.g(frame)
        self.callback.assert_called_once()
        channel_id, method_sig, _, msg = self.callback.call_args[0]
        assert channel_id == 2
        assert method_sig == spec.Basic.Deliver
        assert isinstance(msg.body, FileBody)
        assert msg.body.read() == body2

        # the delivered body is owned by the consumer: tearing the
        # connection down does not close it
        self.conn.collect()
        assert not msg.body.closed
        msg.body.close()
        assert msg.body.closed

    def test_connection_close_releases_partial_content(self):
        frames = encode_frames(1, Message(body=b'x' * 100))
        for frame in frames[:-1]:
            self.g(frame)
        assert len(self.sinks) == 1
        self.conn.collect()
        assert self.sinks[0]._file is None
        self.callback.assert_not_called()

    def test_body_frame_exceeding_declared_size(self):
        frames = encode_frames(1, Message(body=b'x' * 100))
        self.g(frames[0])
        self.g(frames[1])
        assert len(self.sinks) == 1
        with pytest.raises(FrameError):
            # 101 bytes received for a 100 byte body
            self.g((3, 1, b'x' * 101))
        # the failed assembly released its sink and reset the channel
        assert self.sinks[0]._file is None
        self.callback.assert_not_called()
        # the channel accepts a fresh message afterwards
        frames = encode_frames(1, Message(body=b'y' * 10))
        for frame in frames:
            self.g(frame)
        msg = self.callback.call_args[0][3]
        assert msg.body == b'y' * 10
