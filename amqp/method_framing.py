"""Convert between frames and higher-level AMQP methods."""
# Copyright (C) 2007-2008 Barry Pederson <bp@barryp.org>

import tempfile
import threading
from collections import defaultdict
from struct import pack, pack_into, unpack_from

from . import spec
from .basic_message import Message
from .exceptions import UnexpectedFrame
from .utils import str_to_bytes

__all__ = (
    'frame_handler', 'frame_writer',
    'MemorySink', 'FileSink', 'FileBody', 'SpooledBodySink',
)

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

#: Default threshold above which :class:`SpooledBodySink` spools the
#: message body to a temporary file instead of keeping it in memory.
DEFAULT_BODY_SINK_THRESHOLD = 1024 * 1024


class MemorySink:
    """Body sink accumulating content frames in memory.

    This is the default sink: the finished body is delivered as
    :class:`bytes`, exactly like the historical behavior.
    """

    def __init__(self, body_size=0):
        self.chunks = []

    def write(self, chunk):
        self.chunks.append(chunk)

    def finish(self):
        chunks, self.chunks = self.chunks, []
        if not chunks:
            return b''
        if len(chunks) == 1:
            return chunks[0]
        return bytes().join(chunks)

    def close(self):
        self.chunks[:] = []


class FileBody:
    """Message body spooled to an anonymous temporary file.

    Large inbound bodies are delivered as this object instead of
    :class:`bytes`.  It provides a minimal file-like interface
    (:meth:`read`/:meth:`close`, usable as a context manager); the
    backing temporary file is anonymous, so no filesystem path is ever
    exposed as the message body.

    Ownership is transferred to the consumer on delivery: closing the
    channel or connection will not close an already delivered body,
    and the consumer is responsible for closing it.
    """

    __slots__ = ('_file',)

    def __init__(self, fileobj):
        self._file = fileobj

    def read(self, size=-1):
        if self._file is None:
            raise ValueError('read from closed message body')
        return self._file.read(size)

    def close(self):
        fileobj, self._file = self._file, None
        if fileobj is not None:
            fileobj.close()

    @property
    def closed(self):
        return self._file is None

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def __repr__(self):
        state = 'closed' if self.closed else 'open'
        return f'<FileBody ({state}) at {id(self):#x}>'


class FileSink:
    """Body sink spooling content frames to a temporary file.

    The finished body is delivered as a :class:`FileBody`.  The
    temporary file is created with :func:`tempfile.TemporaryFile`, so
    it has no visible path and is removed when closed.
    """

    def __init__(self, body_size=0, dir=None):
        self._file = tempfile.TemporaryFile(dir=dir)

    def write(self, chunk):
        self._file.write(chunk)

    def finish(self):
        # Ownership of the file moves to the returned FileBody; this
        # sink no longer refers to it, so closing the sink (or the
        # connection) afterwards cannot invalidate the delivered body.
        fileobj, self._file = self._file, None
        fileobj.seek(0)
        return FileBody(fileobj)

    def close(self):
        fileobj, self._file = self._file, None
        if fileobj is not None:
            fileobj.close()


class SpooledBodySink:
    """Factory selecting a body sink based on the declared body size.

    Bodies whose header-declared size exceeds ``threshold`` bytes are
    spooled to a temporary file (delivered as :class:`FileBody`);
    smaller bodies are kept in memory and delivered as :class:`bytes`.

    Pass an instance as the ``body_sink`` argument of
    :class:`amqp.Connection`::

        Connection(..., body_sink=SpooledBodySink(threshold=1024 * 1024))
    """

    def __init__(self, threshold=DEFAULT_BODY_SINK_THRESHOLD, dir=None):
        self.threshold = threshold
        self.dir = dir

    def __call__(self, body_size):
        if body_size is not None and body_size > self.threshold:
            return FileSink(body_size, dir=self.dir)
        return MemorySink(body_size)


def frame_handler(connection, callback,
                  unpack_from=unpack_from, content_methods=_CONTENT_METHODS):
    """Create closure that reads frames."""
    expected_types = defaultdict(lambda: 1)
    partial_messages = {}
    # Factory creating one body sink per inbound message; each channel
    # being assembled gets its own sink instance.
    sink_factory = getattr(connection, 'body_sink', None) or MemorySink

    def discard_channel(channel):
        """Drop any undelivered content for ``channel``.

        Releases the body sink of a partially received message so that
        temporary files are not leaked when a channel is closed or an
        error occurs mid-assembly.
        """
        msg = partial_messages.pop(channel, None)
        expected_types.pop(channel, None)
        if msg is not None:
            msg._discard_body()

    def on_frame(frame):
        frame_type, channel, buf = frame
        connection.bytes_recv += 1
        if frame_type not in (expected_types[channel], 8):
            raise UnexpectedFrame(
                'Received frame {} while expecting type: {}'.format(
                    frame_type, expected_types[channel]),
            )
        elif frame_type == 1:
            method_sig = unpack_from('>HH', buf, 0)

            if method_sig in content_methods:
                # Save what we've got so far and wait for the content-header
                partial_messages[channel] = Message(
                    frame_method=method_sig, frame_args=buf,
                )
                expected_types[channel] = 2
                return False

            callback(channel, method_sig, buf, None)

        elif frame_type == 2:
            msg = partial_messages[channel]
            try:
                msg.inbound_header(buf)
                if not msg.ready:
                    # wait for the content-body
                    expected_types[channel] = 3
                    msg._body_sink = sink_factory(msg.body_size)
                    return False
            except Exception:
                discard_channel(channel)
                raise

            # bodyless message, we're done
            expected_types[channel] = 1
            partial_messages.pop(channel, None)
            callback(channel, msg.frame_method, msg.frame_args, msg)

        elif frame_type == 3:
            msg = partial_messages[channel]
            try:
                msg.inbound_body(buf)
            except Exception:
                discard_channel(channel)
                raise
            if not msg.ready:
                # wait for the rest of the content-body
                return False
            expected_types[channel] = 1
            partial_messages.pop(channel, None)
            callback(channel, msg.frame_method, msg.frame_args, msg)
        elif frame_type == 8:
            # bytes_recv already updated
            return False
        return True

    def close():
        """Discard all undelivered content (connection teardown)."""
        for channel in list(partial_messages):
            discard_channel(channel)

    on_frame.discard_channel = discard_channel
    on_frame.close = close
    return on_frame


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
