"""Convert between frames and higher-level AMQP methods."""
# Copyright (C) 2007-2008 Barry Pederson <bp@barryp.org>

import threading
from collections import defaultdict
from struct import pack, pack_into, unpack_from

from . import spec
from .basic_message import Message
from .exceptions import UnexpectedFrame
from .utils import str_to_bytes

__all__ = ('FrameHandler', 'frame_handler', 'frame_writer')

#: Set of methods that require both a content frame and a body frame.
_CONTENT_METHODS = frozenset([
    spec.Basic.Return,
    spec.Basic.Deliver,
    spec.Basic.GetOk,
])


#: Number of bytes reserved for protocol in a content frame.
#: We use this to calculate when a frame exceeeds the max frame size,
#: and if it does not the message will fit into the preallocated buffer.
FRAME_OVERHEAD = 40


class FrameHandler:
    """Process inbound frames, assembling content-bearing methods.

    The handler is callable and processes one frame at a time; it
    returns True when a complete method (with any content) was
    delivered to the callback.

    If ``body_sink_factory`` is given it is called as
    ``factory(body_size, channel)`` when the content header of a
    message arrives, and must return a body sink object (with
    ``write``/``finalize``/``close`` methods, see
    :mod:`amqp.body_sink`) or None to keep the default in-memory
    behavior.  Each channel gets its own sink, so messages interleaved
    on different channels are assembled independently.

    Once a message is complete the sink is finalized and ownership of
    the resulting body object (e.g. :class:`amqp.body_sink.FileBody`)
    is transferred to the receiver of the message.  Undelivered
    content is released by :meth:`discard` (single channel) and
    :meth:`close` (all channels).
    """

    def __init__(self, connection, callback,
                 unpack_from=unpack_from, content_methods=_CONTENT_METHODS,
                 body_sink_factory=None):
        self.connection = connection
        self.callback = callback
        self.unpack_from = unpack_from
        self.content_methods = content_methods
        self.body_sink_factory = body_sink_factory
        self.expected_types = defaultdict(lambda: 1)
        self.partial_messages = {}

    def set_body_sink_factory(self, body_sink_factory):
        """Set the factory used to create body sinks for new messages."""
        self.body_sink_factory = body_sink_factory

    def __call__(self, frame):
        frame_type, channel, buf = frame
        connection = self.connection
        expected_types = self.expected_types
        partial_messages = self.partial_messages
        connection.bytes_recv += 1
        if frame_type not in (expected_types[channel], 8):
            raise UnexpectedFrame(
                'Received frame {} while expecting type: {}'.format(
                    frame_type, expected_types[channel]),
            )
        elif frame_type == 1:
            method_sig = self.unpack_from('>HH', buf, 0)

            if method_sig in self.content_methods:
                # Save what we've got so far and wait for the content-header
                partial_messages[channel] = Message(
                    frame_method=method_sig, frame_args=buf,
                )
                expected_types[channel] = 2
                return False

            self.callback(channel, method_sig, buf, None)

        elif frame_type == 2:
            msg = partial_messages[channel]
            msg.inbound_header(buf)

            if not msg.ready:
                self._attach_body_sink(msg, channel)
                # wait for the content-body
                expected_types[channel] = 3
                return False

            # bodyless message, we're done
            expected_types[channel] = 1
            partial_messages.pop(channel, None)
            self.callback(channel, msg.frame_method, msg.frame_args, msg)

        elif frame_type == 3:
            msg = partial_messages[channel]
            try:
                msg.inbound_body(buf)
            except Exception:
                # Assembly failed: release the undelivered content.
                self.discard(channel)
                raise
            if not msg.ready:
                # wait for the rest of the content-body
                return False
            expected_types[channel] = 1
            partial_messages.pop(channel, None)
            self.callback(channel, msg.frame_method, msg.frame_args, msg)
        elif frame_type == 8:
            # bytes_recv already updated
            return False
        return True

    def _attach_body_sink(self, msg, channel):
        factory = self.body_sink_factory
        if factory is not None and msg.body_sink is None:
            sink = factory(msg.body_size, channel)
            if sink is not None:
                msg.body_sink = sink

    def discard(self, channel):
        """Discard a partially received message on ``channel``.

        Releases any resources held by the undelivered content (e.g. a
        temporary file backing the body sink).  Does nothing if no
        message is being assembled on the channel.
        """
        msg = self.partial_messages.pop(channel, None)
        self.expected_types.pop(channel, None)
        if msg is not None:
            msg.discard_body()

    def close(self):
        """Discard all partially received messages on every channel."""
        for channel in list(self.partial_messages):
            self.discard(channel)


def frame_handler(connection, callback,
                  unpack_from=unpack_from, content_methods=_CONTENT_METHODS,
                  body_sink_factory=None):
    """Create the inbound frame processor (see :class:`FrameHandler`)."""
    return FrameHandler(
        connection, callback,
        unpack_from=unpack_from, content_methods=content_methods,
        body_sink_factory=body_sink_factory,
    )


class Buffer:
    def __init__(self, buf):
        self.buf = buf

    @property
    def buf(self):
        return self._buf

    @buf.setter
    def buf(self, buf):
        self._buf = buf
        # Using a memoryview allows slicing without copying underlying data.
        # Slicing this is much faster than slicing the bytearray directly.
        # More details: https://stackoverflow.com/a/34257357
        self.view = memoryview(buf)


def frame_writer(connection, transport,
                 pack=pack, pack_into=pack_into, range=range, len=len,
                 bytes=bytes, str_to_bytes=str_to_bytes, text_t=str):
    """Create closure that writes frames."""
    write = transport.write

    buffer_store = Buffer(bytearray(connection.frame_max - 8))

    # The buffer is shared by every frame written on this connection and is
    # handed to transport.write() as a live memoryview, so two threads
    # publishing on the same connection would overwrite each other's frame
    # before the socket has consumed it (#462).  Looked up at connect time
    # rather than import time so it picks up eventlet/gevent monkeypatching
    # even when amqp was imported before the patch was applied.
    buffer_lock = threading.Lock()

    def write_frame(type_, channel, method_sig, args, content):
        with buffer_lock:
            return _write_frame(type_, channel, method_sig, args, content)

    def _write_frame(type_, channel, method_sig, args, content):
        chunk_size = connection.frame_max - 8
        offset = 0
        properties = None
        args = str_to_bytes(args)
        if content:
            body = content.body
            if isinstance(body, str):
                encoding = content.properties.setdefault(
                    'content_encoding', 'utf-8')
                body = body.encode(encoding)
            properties = content._serialize_properties()
            bodylen = len(body)
            properties_len = len(properties) or 0
            framelen = len(args) + properties_len + bodylen + FRAME_OVERHEAD
            bigbody = framelen > chunk_size
        else:
            body, bodylen, bigbody = None, 0, 0

        if bigbody:
            # ## SLOW: string copy and write for every frame
            frame = (b''.join([pack('>HH', *method_sig), args])
                     if type_ == 1 else b'')  # encode method frame
            framelen = len(frame)
            write(pack('>BHI%dsB' % framelen,
                       type_, channel, framelen, frame, 0xce))
            if body:
                frame = b''.join([
                    pack('>HHQ', method_sig[0], 0, len(body)),
                    properties,
                ])
                framelen = len(frame)
                write(pack('>BHI%dsB' % framelen,
                           2, channel, framelen, frame, 0xce))

                for i in range(0, bodylen, chunk_size):
                    frame = body[i:i + chunk_size]
                    framelen = len(frame)
                    write(pack('>BHI%dsB' % framelen,
                               3, channel, framelen,
                               frame, 0xce))

        else:
            # frame_max can be updated via connection._on_tune. If
            # it became larger, then we need to resize the buffer
            # to prevent overflow.
            if chunk_size > len(buffer_store.buf):
                buffer_store.buf = bytearray(chunk_size)
            buf = buffer_store.buf

            # ## FAST: pack into buffer and single write
            frame = (b''.join([pack('>HH', *method_sig), args])
                     if type_ == 1 else b'')
            framelen = len(frame)
            pack_into('>BHI%dsB' % framelen, buf, offset,
                      type_, channel, framelen, frame, 0xce)
            offset += 8 + framelen
            if body is not None:
                frame = b''.join([
                    pack('>HHQ', method_sig[0], 0, len(body)),
                    properties,
                ])
                framelen = len(frame)

                pack_into('>BHI%dsB' % framelen, buf, offset,
                          2, channel, framelen, frame, 0xce)
                offset += 8 + framelen

                bodylen = len(body)
                if bodylen > 0:
                    framelen = bodylen
                    pack_into('>BHI%dsB' % framelen, buf, offset,
                              3, channel, framelen, body, 0xce)
                    offset += 8 + framelen

            write(buffer_store.view[:offset])

        connection.bytes_sent += 1
    return write_frame
