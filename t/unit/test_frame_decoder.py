from struct import pack

import pytest

from amqp.exceptions import FrameError, UnexpectedFrame
from amqp.frame_decoder import MAX_FRAME_SIZE, FrameDecoder

FRAME_END = b'\xce'


def frame(frame_type, channel, payload):
    header = pack('>BHI', frame_type, channel, len(payload))
    return header + payload + FRAME_END


# heartbeat plus content on multiple channels
HEARTBEAT = frame(8, 0, b'')
METHOD_CH1 = frame(1, 1, b'\x00\x28\x00\x0a' + b'method-payload')
HEADER_CH2 = frame(2, 2, b'\x00' * 12)
BODY_CH1 = frame(3, 1, b'hello channel one')
BODY_CH2 = frame(3, 2, b'and channel two')

STREAM = HEARTBEAT + METHOD_CH1 + HEADER_CH2 + BODY_CH1 + BODY_CH2
EXPECTED = [
    (8, 0, b''),
    (1, 1, b'\x00\x28\x00\x0a' + b'method-payload'),
    (2, 2, b'\x00' * 12),
    (3, 1, b'hello channel one'),
    (3, 2, b'and channel two'),
]


def collect(decoder):
    frames = []
    while True:
        frame_ = decoder.next_frame()
        if frame_ is None:
            return frames
        frames.append(frame_)


class test_FrameDecoder:

    def test_one_shot(self):
        decoder = FrameDecoder()
        decoder.feed(STREAM)
        assert collect(decoder) == EXPECTED
        assert decoder.offset == len(STREAM)
        assert decoder.buffered == 0

    def test_split_at_every_position(self):
        for i in range(len(STREAM) + 1):
            decoder = FrameDecoder()
            decoder.feed(STREAM[:i])
            decoder.feed(STREAM[i:])
            assert collect(decoder) == EXPECTED, f'split at {i}'
            assert decoder.offset == len(STREAM)

    def test_byte_by_byte(self):
        decoder = FrameDecoder()
        frames = []
        for i in range(len(STREAM)):
            decoder.feed(STREAM[i:i + 1])
            frames.extend(collect(decoder))
        assert frames == EXPECTED

    def test_needs_more_data(self):
        decoder = FrameDecoder()
        decoder.feed(METHOD_CH1[:10])
        assert decoder.next_frame() is None
        assert decoder.expected_bytes == len(METHOD_CH1) - 10

    def test_expected_bytes_header(self):
        decoder = FrameDecoder()
        decoder.feed(STREAM[:3])
        assert decoder.expecting_header
        assert decoder.expected_bytes == 4
        decoder.feed(STREAM[3:7])
        assert not decoder.expecting_header
        assert decoder.expected_bytes == len(HEARTBEAT) - 7

    def test_offset_tracks_consumed_frames(self):
        decoder = FrameDecoder()
        decoder.feed(HEARTBEAT + METHOD_CH1[:5])
        assert decoder.next_frame() == (8, 0, b'')
        assert decoder.offset == len(HEARTBEAT)
        assert decoder.next_frame() is None
        assert decoder.offset == len(HEARTBEAT)

    def test_malicious_size_rejected_from_header_only(self):
        decoder = FrameDecoder()
        # only a header declaring a gigantic payload, no payload at all
        decoder.feed(pack('>BHI', 1, 1, 0xFFFFFFFF))
        with pytest.raises(FrameError):
            decoder.next_frame()

    def test_hard_limit_boundary(self):
        decoder = FrameDecoder()
        decoder.feed(pack('>BHI', 1, 1, MAX_FRAME_SIZE))
        # exactly at the limit: not rejected, just incomplete
        assert decoder.next_frame() is None
        decoder = FrameDecoder()
        decoder.feed(pack('>BHI', 1, 1, MAX_FRAME_SIZE + 1))
        with pytest.raises(FrameError):
            decoder.next_frame()

    def test_frame_max_rejected_from_header_only(self):
        decoder = FrameDecoder(frame_max=128)
        decoder.feed(pack('>BHI', 1, 1, 129))
        with pytest.raises(FrameError):
            decoder.next_frame()

    def test_frame_max_may_be_negotiated_later(self):
        decoder = FrameDecoder()
        decoder.feed(frame(1, 1, b'x' * 64))
        assert decoder.next_frame() == (1, 1, b'x' * 64)
        decoder.frame_max = 32
        decoder.feed(frame(1, 1, b'x' * 64))
        with pytest.raises(FrameError):
            decoder.next_frame()

    def test_eof_clean(self):
        decoder = FrameDecoder()
        decoder.feed(STREAM)
        assert collect(decoder) == EXPECTED
        decoder.eof()  # no partial frame pending: clean end

    def test_eof_mid_frame(self):
        decoder = FrameDecoder()
        decoder.feed(STREAM[:-1])
        with pytest.raises(UnexpectedFrame):
            decoder.eof()

    def test_bad_frame_end_is_sticky(self):
        bad = pack('>BHI', 1, 1, 4) + b'abcd' + b'\x13'
        decoder = FrameDecoder()
        decoder.feed(bad + HEARTBEAT)
        with pytest.raises(UnexpectedFrame) as exc:
            decoder.next_frame()
        assert exc.value.code == 505
        # remaining bytes must not be mistaken for a new frame
        with pytest.raises(UnexpectedFrame):
            decoder.next_frame()
        with pytest.raises(UnexpectedFrame):
            decoder.feed(HEARTBEAT)

    def test_failure_is_sticky(self):
        decoder = FrameDecoder()
        decoder.feed(pack('>BHI', 1, 1, 0xFFFFFFFF))
        with pytest.raises(FrameError):
            decoder.next_frame()
        with pytest.raises(FrameError):
            decoder.next_frame()

    def test_reset(self):
        decoder = FrameDecoder()
        decoder.feed(pack('>BHI', 1, 1, 0xFFFFFFFF))
        with pytest.raises(FrameError):
            decoder.next_frame()
        decoder.reset()
        assert decoder.offset == 0
        decoder.feed(HEARTBEAT)
        assert decoder.next_frame() == (8, 0, b'')
