import socket
from collections import deque
from contextlib import contextmanager
from struct import pack
from unittest.mock import Mock

import pytest

from amqp import Connection, spec
import amqp.connection as connection_module
from amqp.basic_message import Message
from amqp.channel import Channel
from amqp.method_framing import frame_handler
from amqp.sasl import PLAIN
from amqp.serialization import dumps
from amqp.shutdown import PendingPublish


class FakeClock:
    def __init__(self):
        self.time = 100.0

    def monotonic(self):
        return self.time

    def advance(self, seconds):
        self.time += seconds


class FakeTransport:
    def __init__(self, clock, responses):
        self.clock = clock
        self.responses = responses
        self.timeouts = []
        self.read_deadlines = []
        self.closed = False

    @contextmanager
    def having_timeout(self, timeout):
        self.timeouts.append(timeout)
        self.read_deadlines.append(
            self.clock.time + timeout if timeout is not None else None
        )
        yield self

    def read_frame(self):
        if self.responses:
            return self.responses.popleft()
        timeout = self.timeouts[-1] or 0
        self.clock.advance(timeout)
        raise socket.timeout()

    def close(self):
        self.closed = True


def method_frame(channel, method_sig, args=b''):
    return 1, channel, pack('>HH', *method_sig) + args


class FrameScript:
    def __init__(self, transport):
        self.transport = transport
        self.responses = transport.responses
        self.outgoing = []
        self.pending_acks = {}

    def queue_ack(self, channel, delivery_tag):
        self.pending_acks.setdefault(channel, []).append(method_frame(
            channel, spec.Basic.Ack,
            dumps('Lb', (delivery_tag, False)),
        ))

    def writer(self, frame_type, channel, method_sig, args, content):
        self.outgoing.append((frame_type, channel, method_sig, args, content))
        if method_sig == spec.Channel.Open:
            self.responses.append(method_frame(channel, spec.Channel.OpenOk))
        elif method_sig == spec.Confirm.Select:
            self.responses.append(
                method_frame(channel, spec.Confirm.SelectOk)
            )
        elif method_sig == spec.Basic.Consume:
            queue_length = args[2]
            consumer_offset = 3 + queue_length
            consumer_length = args[consumer_offset]
            consumer_tag = args[
                consumer_offset + 1:
                consumer_offset + 1 + consumer_length
            ].decode()
            self.responses.append(method_frame(
                channel, spec.Basic.ConsumeOk, dumps('s', (consumer_tag,))
            ))
        elif method_sig == spec.Basic.Cancel:
            consumer_tag = args[1:args[0] + 1].decode()
            self.responses.append(method_frame(
                channel, spec.Basic.CancelOk, dumps('s', (consumer_tag,))
            ))
            for pending_ack in self.pending_acks.pop(channel, []):
                self.responses.append(pending_ack)
        elif method_sig == spec.Channel.Close:
            self.responses.append(
                method_frame(channel, spec.Channel.CloseOk)
            )
        elif method_sig == spec.Connection.Close:
            self.responses.append(method_frame(
                0, spec.Connection.CloseOk, dumps('BsBB', (0, '', 0, 0))
            ))

    def graceful_methods(self):
        return [
            (channel, method_sig)
            for _, channel, method_sig, _, _ in self.outgoing
            if method_sig in (
                spec.Basic.Cancel, spec.Channel.Close,
                spec.Connection.Close,
            )
        ]


def make_connection(clock):
    responses = deque()
    transport = FakeTransport(clock, responses)
    conn = Connection(
        authentication=PLAIN('guest', 'guest'),
        frame_writer=Mock(name='frame_writer_cls'),
    )
    script = FrameScript(transport)
    conn._transport = transport
    conn.frame_writer = script.writer
    conn.on_inbound_frame = frame_handler(conn, conn.on_inbound_method)
    conn._handshake_complete = True
    return conn, transport, script


def make_channel(conn, channel_id, consumer_tag):
    channel = Channel(conn, channel_id)
    channel.open()
    channel.confirm_select()
    channel.basic_consume('queue', consumer_tag=consumer_tag,
                          callback=Mock())
    channel._basic_publish(Message('payload'), exchange='exchange',
                           routing_key='routing-key')
    return channel


def test_graceful_close_waits_for_confirms_then_closes_in_order(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(connection_module, 'monotonic', clock.monotonic)
    conn, transport, script = make_connection(clock)
    channel = make_channel(conn, 1, 'consumer-1')
    script.queue_ack(1, 1)

    result = conn.graceful_close(timeout=10)

    assert result.complete
    assert result.unconfirmed_publishes == ()
    assert result.uncancelled_consumers == ()
    assert transport.closed
    assert script.graceful_methods() == [
        (1, spec.Basic.Cancel),
        (1, spec.Channel.Close),
        (0, spec.Connection.Close),
    ]
    assert channel.connection is None


def test_graceful_close_uses_one_deadline_and_reports_missing_confirms(
        monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(connection_module, 'monotonic', clock.monotonic)
    conn, transport, script = make_connection(clock)
    make_channel(conn, 1, 'consumer-1')
    make_channel(conn, 2, 'consumer-2')
    script.queue_ack(1, 1)

    result = conn.graceful_close(timeout=10)

    assert result.timed_out
    assert result.unconfirmed_publishes == (
        PendingPublish(2, 1, 'exchange', 'routing-key'),
    )
    assert result.uncancelled_consumers == ()
    assert transport.closed
    assert clock.time == 110.0
    assert transport.timeouts[-3:] == [10, 10, 10]
    assert transport.timeouts[-4:] == [10, 10, 10, 10]
    graceful_read_deadlines = transport.read_deadlines[8:]
    assert graceful_read_deadlines
    assert set(graceful_read_deadlines) == {110.0}
    assert script.graceful_methods() == [
        (1, spec.Basic.Cancel),
        (2, spec.Basic.Cancel),
        (1, spec.Channel.Close),
        (2, spec.Channel.Close),
        (0, spec.Connection.Close),
    ]


def test_graceful_close_accepts_heartbeat_ack_and_cancel(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(connection_module, 'monotonic', clock.monotonic)
    conn, _, script = make_connection(clock)
    make_channel(conn, 1, 'consumer-1')
    script.responses.appendleft((8, 0, b''))
    script.queue_ack(1, 1)

    result = conn.graceful_close(timeout=10)

    assert result.complete
    assert conn.bytes_recv >= 3


def test_graceful_close_callback_error_does_not_prevent_transport_close(
        monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(connection_module, 'monotonic', clock.monotonic)
    conn, transport, script = make_connection(clock)
    channel = make_channel(conn, 1, 'consumer-1')
    channel.cancel_callbacks['consumer-1'] = Mock(
        side_effect=RuntimeError('callback failed'))
    script.queue_ack(1, 1)

    result = conn.graceful_close(timeout=10)

    assert not result.complete
    assert isinstance(result.errors[0], RuntimeError)
    assert transport.closed


def test_graceful_close_rejects_new_publish_and_consumers(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(connection_module, 'monotonic', clock.monotonic)
    conn, _, script = make_connection(clock)
    channel = make_channel(conn, 1, 'consumer-1')
    script.queue_ack(1, 1)

    def reject_new_work(*args, **kwargs):
        with pytest.raises(Exception):
            channel.basic_consume('queue', consumer_tag='new')
        with pytest.raises(Exception):
            channel._basic_publish(
                Message('new'), exchange='exchange', routing_key='new')

    channel.cancel_callbacks['consumer-1'] = Mock(side_effect=reject_new_work)
    result = conn.graceful_close(timeout=10)

    assert result.complete
    assert 'new' not in channel.callbacks


def test_repeated_graceful_close_shares_terminal_result(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(connection_module, 'monotonic', clock.monotonic)
    conn, transport, _ = make_connection(clock)
    make_channel(conn, 1, 'consumer-1')

    result = conn.graceful_close(timeout=0)
    repeated = conn.graceful_close(timeout=100)

    assert result is repeated
    assert result.timed_out
    assert transport.closed


def test_immediate_close_remains_separate_entry(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(connection_module, 'monotonic', clock.monotonic)
    conn, transport, script = make_connection(clock)

    conn.close()

    assert (0, spec.Connection.Close) in script.graceful_methods()
    assert transport.closed


def test_graceful_close_reports_nack_as_error_not_unconfirmed(monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr(connection_module, 'monotonic', clock.monotonic)
    conn, transport, script = make_connection(clock)
    channel = make_channel(conn, 1, 'consumer-1')
    script.pending_acks[1] = [method_frame(
        1, spec.Basic.Nack, dumps('Lb', (1, False)),
    )]

    result = conn.graceful_close(timeout=10)

    assert not result.complete
    assert result.unconfirmed_publishes == ()
    assert result.errors
    assert transport.closed
    assert channel.connection is None
