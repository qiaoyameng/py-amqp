"""Sinks for assembling inbound message bodies.

By default inbound content bodies are assembled in memory and delivered
as ``bytes``.  Consumers that occasionally receive very large messages
can configure a *body sink* on the frame handler (see
:func:`amqp.method_framing.frame_handler`) so that bodies exceeding a
threshold are written to a temporary file instead.
"""
import io
import tempfile

__all__ = (
    'DEFAULT_BODY_SPOOL_SIZE',
    'FileBody',
    'BytesBodySink',
    'SpooledBodySink',
    'spooled_body_sink',
)

#: Default threshold (in bytes) above which a body is spooled to disk.
DEFAULT_BODY_SPOOL_SIZE = 1024 * 1024  # 1 MiB


class FileBody:
    """Message body stored in a temporary file.

    Delivered as ``Message.body`` when an inbound body was spooled to
    disk.  Provides a minimal file-like interface (``read``/``seek``/
    ``tell``/``close``); the temporary file path is deliberately not
    exposed as the message body.

    Ownership is transferred to the receiver of the message: call
    :meth:`close` when done, or use the object as a context manager.
    """

    __slots__ = ('_file',)

    def __init__(self, fileobj):
        self._file = fileobj

    def read(self, size=-1):
        """Read up to ``size`` bytes (or everything) from the body."""
        return self._file.read(size)

    def seek(self, offset, whence=io.SEEK_SET):
        return self._file.seek(offset, whence)

    def tell(self):
        return self._file.tell()

    @property
    def closed(self):
        return self._file is None or self._file.closed

    def close(self):
        """Close the body and release the underlying temporary file."""
        fileobj, self._file = self._file, None
        if fileobj is not None:
            fileobj.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def __repr__(self):
        return '<FileBody at {:#x} (closed={})>'.format(id(self), self.closed)


class BytesBodySink:
    """Body sink assembling the body in memory.

    Finalizes to ``bytes``, preserving the historical behavior for
    small messages.
    """

    __slots__ = ('_chunks',)

    def __init__(self):
        self._chunks = []

    def write(self, chunk):
        self._chunks.append(chunk)

    def finalize(self):
        """Return the assembled body, transferring ownership.

        The sink must not be used after this call.
        """
        chunks, self._chunks = self._chunks, None
        return b''.join(chunks)

    def close(self):
        """Discard buffered content without delivering it."""
        self._chunks = None


class SpooledBodySink:
    """Body sink spooling to a temporary file above ``max_size`` bytes.

    Bodies of at most ``max_size`` bytes are kept in memory and
    finalized to ``bytes``; larger bodies roll over to a temporary file
    and finalize to a :class:`FileBody`.
    """

    __slots__ = ('max_size', 'dir', '_size', '_file', '_rolled')

    def __init__(self, max_size=DEFAULT_BODY_SPOOL_SIZE, dir=None):
        self.max_size = max_size
        self.dir = dir
        self._size = 0
        self._file = io.BytesIO()
        self._rolled = False

    def write(self, chunk):
        self._size += len(chunk)
        if not self._rolled and self._size > self.max_size:
            rolled = tempfile.TemporaryFile(mode='w+b', dir=self.dir)
            rolled.write(self._file.getvalue())
            self._file.close()
            self._file = rolled
            self._rolled = True
        self._file.write(chunk)

    def finalize(self):
        """Return the assembled body, transferring ownership.

        Returns ``bytes`` for in-memory bodies and :class:`FileBody`
        for bodies that rolled over to a temporary file.  The sink must
        not be used after this call.
        """
        fileobj, self._file = self._file, None
        if self._rolled:
            fileobj.seek(0)
            return FileBody(fileobj)
        try:
            return fileobj.getvalue()
        finally:
            fileobj.close()

    def close(self):
        """Discard buffered content and release any temporary file."""
        fileobj, self._file = self._file, None
        if fileobj is not None:
            fileobj.close()


def spooled_body_sink(max_size=DEFAULT_BODY_SPOOL_SIZE, dir=None):
    """Create a body sink factory spooling large bodies to disk.

    The returned callable accepts ``(body_size, channel)`` and returns
    a :class:`SpooledBodySink`.  Suitable as the ``body_sink_factory``
    argument of :func:`amqp.method_framing.frame_handler` and
    :class:`amqp.Connection`.
    """
    def factory(body_size, channel=None):
        return SpooledBodySink(max_size=max_size, dir=dir)
    return factory
