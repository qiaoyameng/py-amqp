"""Incremental AMQP frame decoder."""
# Copyright (C) 2026 py-amqp contributors

from struct import unpack

from .exceptions import FrameError, UnexpectedFrame

__all__ = ('FrameDecoder', 'FRAME_END', 'MAX_FRAME_SIZE')

#: Value of the frame-end octet defined by AMQP 0-9-1.
FRAME_END = 0xCE

#: Hard upper bound for a single frame payload.
#:
#: The 4 octet frame header length field is attacker controlled, so a
#: declared payload size is never trusted beyond this limit, even when no
#: ``frame_max`` has been negotiated with the peer yet.  This matches the
#: largest value the socket layer is able to receive in a single call.
MAX_FRAME_SIZE = 0x7FFFFFFF

#: Size of the frame header: type (octet), channel (short), size (long).
FRAME_HEADER_SIZE = 7


class FrameDecoder:
    """Incrementally decode AMQP 0-9-1 frames from a byte stream.

    Allows parsing frames from any source of network chunks without
    going through a socket::

        decoder = FrameDecoder(frame_max=131072)
        decoder.feed(chunk)
        while True:
            frame = decoder.next_frame()
            if frame is None:
                break  # need more data
            frame_type, channel, payload = frame

    PARAMETERS:
        frame_max: int|None

            Negotiated maximum frame payload size.  Declared sizes larger
            than this are rejected as soon as the frame header is
            available.  May be (re)assigned at any time, e.g. after
            ``connection.tune`` completes.

    ATTRIBUTES:
        offset: int

            Connection byte offset: number of bytes consumed by complete
            frames since the decoder was created (or last reset).

    """

    def __init__(self, frame_max=None):
        self.frame_max = frame_max
        self._buffer = bytearray()
        self.offset = 0
        self._failed = None

    def reset(self):
        """Drop all buffered data and any latched failure."""
        self._buffer = bytearray()
        self.offset = 0
        self._failed = None

    @property
    def buffered(self):
        """Number of fed bytes not yet consumed by a complete frame."""
        return len(self._buffer)

    @property
    def expecting_header(self):
        """True if not even a full frame header has been received yet."""
        return len(self._buffer) < FRAME_HEADER_SIZE

    @property
    def expected_bytes(self):
        """Number of additional bytes needed to complete the current frame.

        Only meaningful when :meth:`next_frame` would return ``None``.
        """
        have = len(self._buffer)
        if have < FRAME_HEADER_SIZE:
            return FRAME_HEADER_SIZE - have
        size = unpack('>I', self._buffer[3:FRAME_HEADER_SIZE])[0]
        return FRAME_HEADER_SIZE + size + 1 - have

    def feed(self, data):
        """Append an arbitrary chunk of network data to the decoder."""
        if self._failed is not None:
            raise self._failed
        if data:
            self._buffer += data

    def next_frame(self):
        """Try to decode one frame from the buffered data.

        Returns:
            tuple: ``(frame_type, channel, payload)`` when a complete
                frame is available, ``None`` when more data is needed.

        Raises:
            amqp.exceptions.FrameError: on oversized declared payloads.
            amqp.exceptions.UnexpectedFrame: on bad frame-end octets.
                The failure is latched: once raised, all subsequent calls
                re-raise the same error and the remaining buffered bytes
                are never mistaken for a new frame.
        """
        if self._failed is not None:
            raise self._failed
        buf = self._buffer
        if len(buf) < FRAME_HEADER_SIZE:
            return None
        frame_type, channel, size = unpack('>BHI', buf[:FRAME_HEADER_SIZE])
        # Reject oversized declarations as soon as the header is in,
        # without waiting for the payload to arrive.
        if size > MAX_FRAME_SIZE:
            self._fail(FrameError(
                f'Frame size {size} exceeds hard limit {MAX_FRAME_SIZE}'))
        if self.frame_max is not None and size > self.frame_max:
            self._fail(FrameError(
                f'Frame size {size} exceeds negotiated '
                f'frame_max {self.frame_max}'))
        frame_size = FRAME_HEADER_SIZE + size + 1
        if len(buf) < frame_size:
            return None
        payload = bytes(buf[FRAME_HEADER_SIZE:FRAME_HEADER_SIZE + size])
        frame_end = buf[FRAME_HEADER_SIZE + size]
        del buf[:frame_size]
        self.offset += frame_size
        if frame_end != FRAME_END:
            self._fail(UnexpectedFrame(
                f'Received frame_end {frame_end:#04x} while expecting 0xce'))
        return frame_type, channel, payload

    def eof(self):
        """Signal that no more data will arrive.

        A clean end of stream (no partial frame pending) returns
        silently; a half-received frame raises
        :class:`amqp.exceptions.UnexpectedFrame`.
        """
        if self._failed is not None:
            raise self._failed
        if self._buffer:
            self._fail(UnexpectedFrame(
                f'End of stream with {len(self._buffer)} bytes '
                'of an incomplete frame'))

    def _fail(self, exc):
        self._failed = exc
        raise exc
