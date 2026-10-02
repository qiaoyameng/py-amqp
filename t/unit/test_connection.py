import re
import socket
import warnings
from collections import deque
from contextlib import contextmanager
from struct import pack, unpack_from
from unittest.mock import Mock, call, patch

import pytest
import time

from amqp import Connection, spec
from amqp.basic_message import Message
from amqp.channel import Channel
from amqp.connection import SSLError
from amqp.exceptions import (Blocked, ConnectionError, NotFound,
                             RecoverableConnectionError, ResourceError)
from amqp.method_framing import frame_handler, frame_writer
from amqp.sasl import AMQPLAIN, EXTERNAL, GSSAPI, PLAIN, SASL
from amqp.serialization import dumps
from amqp.transport import TCPTransport

from t.mocks import ContextMock


class test_Connection:

    @pytest.fixture(autouse=True)
    def setup_conn(self):
        self.frame_handler = Mock(name='frame_handler')
        self.frame_writer = Mock(name='frame_writer_cls')
        self.conn = Connection(
            frame_handler=self.frame_handler,
            frame_writer=self.frame_writer,
            authentication=AMQPLAIN('foo', 'bar'),
        )
        self.conn.Channel = Mock(name='Channel')
        self.conn.Transport = Mock(name='Transport')
        self.conn.transport = self.conn.Transport.return_value
        self.conn.send_method = Mock(name='send_method')
        self.conn.frame_writer = Mock(name='frame_writer')

    def test_sasl_authentication(self):
        authentication = SASL()
        self.conn = Connection(authentication=authentication)
        assert self.conn.authentication == (authentication,)

    def test_sasl_authentication_iterable(self):
        authentication = SASL()
        self.conn = Connection(authentication=(authentication,))
        assert self.conn.authentication == (authentication,)

    def test_gssapi(self):
        self.conn = Connection()
        assert isinstance(self.conn.authentication[0], GSSAPI)

    def test_external(self):
        self.conn = Connection()
        assert isinstance(self.conn.authentication[1], EXTERNAL)

    def test_amqplain(self):
        self.conn = Connection(userid='foo', password='bar')
        auth = self.conn.authentication[2]
        assert isinstance(auth, AMQPLAIN)
        assert auth.username == 'foo'
        assert auth.password == 'bar'

    def test_plain(self):
        self.conn = Connection(userid='foo', password='bar')
        auth = self.conn.authentication[3]
        assert isinstance(auth, PLAIN)
        assert auth.username == 'foo'
        assert auth.password == 'bar'

    def test_login_method_gssapi(self):
        try:
            self.conn = Connection(userid=None, password=None,
                                   login_method='GSSAPI')
        except NotImplementedError:
            pass
        else:
            auths = self.conn.authentication
            assert len(auths) == 1
            assert isinstance(auths[0], GSSAPI)

    def test_login_method_external(self):
        self.conn = Connection(userid=None, password=None,
                               login_method='EXTERNAL')
        auths = self.conn.authentication
        assert len(auths) == 1
        assert isinstance(auths[0], EXTERNAL)

    def test_login_method_amqplain(self):
        self.conn = Connection(login_method='AMQPLAIN')
        auths = self.conn.authentication
        assert len(auths) == 1
        assert isinstance(auths[0], AMQPLAIN)

    def test_login_method_plain(self):
        self.conn = Connection(login_method='PLAIN')
        auths = self.conn.authentication
        assert len(auths) == 1
        assert isinstance(auths[0], PLAIN)

    def test_enter_exit(self):
        self.conn.connect = Mock(name='connect')
        self.conn.close = Mock(name='close')
        with self.conn:
            self.conn.connect.assert_called_with()
        self.conn.close.assert_called_with()

    def test__enter__socket_error(self):
        # test when entering
        self.conn = Connection()
        self.conn.close = Mock(name='close')
        reached = False
        with patch('socket.socket', side_effect=socket.error):
            with pytest.raises(socket.error):
                with self.conn:
                    reached = True
        assert not reached and not self.conn.close.called
        assert self.conn._transport is None and not self.conn.connected

    def test__exit__socket_error(self):
        # test when exiting
        connection = self.conn
        transport = connection._transport
        transport.connected = True
        connection.send_method = Mock(name='send_method',
                                      side_effect=socket.error)
        reached = False
        with pytest.raises(socket.error):
            with connection:
                reached = True
        assert reached
        assert connection.send_method.called and transport.close.called
        assert self.conn._transport is None and not self.conn.connected

    def test_then(self):
        self.conn.on_open = Mock(name='on_open')
        on_success = Mock(name='on_success')
        on_error = Mock(name='on_error')
        self.conn.then(on_success, on_error)
        self.conn.on_open.then.assert_called_with(on_success, on_error)

    def test_connect(self):
        self.conn.transport.connected = False
        self.conn.drain_events = Mock(name='drain_events')

        def on_drain(*args, **kwargs):
            self.conn._handshake_complete = True
        self.conn.drain_events.side_effect = on_drain
        self.conn.connect()
        self.conn.Transport.assert_called_with(
            self.conn.host, self.conn.connect_timeout, self.conn.ssl,
            self.conn.read_timeout, self.conn.write_timeout,
            socket_settings=self.conn.socket_settings,
        )

    def test_connect__already_connected(self):
        callback = Mock(name='callback')
        self.conn.transport.connected = True
        assert self.conn.connect(callback) == callback.return_value
        callback.assert_called_with()

    def test_connect__socket_error(self):
        # check Transport.Connect error
        # socket.error derives from IOError
        # ssl.SSLError derives from socket.error
        self.conn = Connection()
        self.conn.Transport = Mock(name='Transport')
        transport = self.conn.Transport.return_value
        transport.connect.side_effect = IOError
        assert self.conn._transport is None and not self.conn.connected
        with pytest.raises(IOError):
            self.conn.connect()
        transport.connect.assert_called
        assert self.conn._transport is None and not self.conn.connected

    def test_on_start(self):
        self.conn._on_start(3, 4, {'foo': 'bar'}, b'x y z AMQPLAIN PLAIN',
                            'en_US en_GB')
        assert self.conn.version_major == 3
        assert self.conn.version_minor == 4
        assert self.conn.server_properties == {'foo': 'bar'}
        assert self.conn.mechanisms == [b'x', b'y', b'z',
                                        b'AMQPLAIN', b'PLAIN']
        assert self.conn.locales == ['en_US', 'en_GB']
        self.conn.send_method.assert_called_with(
            spec.Connection.StartOk, 'FsSs', (
                self.conn.client_properties, b'AMQPLAIN',
                self.conn.authentication[0].start(self.conn), self.conn.locale,
            ),
        )

    def test_on_start_string_mechanisms(self):
        self.conn._on_start(3, 4, {'foo': 'bar'}, 'x y z AMQPLAIN PLAIN',
                            'en_US en_GB')
        assert self.conn.version_major == 3
        assert self.conn.version_minor == 4
        assert self.conn.server_properties == {'foo': 'bar'}
        assert self.conn.mechanisms == [b'x', b'y', b'z',
                                        b'AMQPLAIN', b'PLAIN']
        assert self.conn.locales == ['en_US', 'en_GB']
        self.conn.send_method.assert_called_with(
            spec.Connection.StartOk, 'FsSs', (
                self.conn.client_properties, b'AMQPLAIN',
                self.conn.authentication[0].start(self.conn), self.conn.locale,
            ),
        )

    def test_missing_credentials(self):
        with pytest.raises(ValueError):
            self.conn = Connection(userid=None, password=None,
                                   login_method='AMQPLAIN')
        with pytest.raises(ValueError):
            self.conn = Connection(password=None, login_method='PLAIN')

    def test_invalid_method(self):
        with pytest.raises(ValueError):
            self.conn = Connection(login_method='any')

    def test_mechanism_mismatch(self):
        with pytest.raises(ConnectionError):
            self.conn._on_start(3, 4, {'foo': 'bar'}, b'x y z',
                                'en_US en_GB')

    def test_login_method_response(self):
        # An old way of doing things.:
        login_method, login_response = b'foo', b'bar'
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.conn = Connection(login_method=login_method,
                                   login_response=login_response)
            self.conn.send_method = Mock(name='send_method')
            self.conn._on_start(3, 4, {'foo': 'bar'}, login_method,
                                'en_US en_GB')
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)

        self.conn.send_method.assert_called_with(
            spec.Connection.StartOk, 'FsSs', (
                self.conn.client_properties, login_method,
                login_response, self.conn.locale,
            ),
        )

    def test_on_start__consumer_cancel_notify(self):
        self.conn._on_start(
            3, 4, {'capabilities': {'consumer_cancel_notify': 1}},
            b'AMQPLAIN', '',
        )
        cap = self.conn.client_properties['capabilities']
        assert cap['consumer_cancel_notify']

    def test_on_start__connection_blocked(self):
        self.conn._on_start(
            3, 4, {'capabilities': {'connection.blocked': 1}},
            b'AMQPLAIN', '',
        )
        cap = self.conn.client_properties['capabilities']
        assert cap['connection.blocked']

    def test_on_start__authentication_failure_close(self):
        self.conn._on_start(
            3, 4, {'capabilities': {'authentication_failure_close': 1}},
            b'AMQPLAIN', '',
        )
        cap = self.conn.client_properties['capabilities']
        assert cap['authentication_failure_close']

    def test_on_start__authentication_failure_close__disabled(self):
        self.conn._on_start(
            3, 4, {'capabilities': {}},
            b'AMQPLAIN', '',
        )
        assert 'capabilities' not in self.conn.client_properties

    def test_on_secure(self):
        self.conn._on_secure('vfz')

    def test_on_tune(self):
        self.conn.client_heartbeat = 16
        self.conn._on_tune(345, 16, 10)
        assert self.conn.channel_max == 345
        assert self.conn.frame_max == 16
        assert self.conn.server_heartbeat == 10
        assert self.conn.heartbeat == 10
        self.conn.send_method.assert_called_with(
            spec.Connection.TuneOk, 'BlB', (
                self.conn.channel_max, self.conn.frame_max,
                self.conn.heartbeat,
            ),
            callback=self.conn._on_tune_sent,
        )

    def test_on_tune__client_heartbeat_disabled(self):
        self.conn.client_heartbeat = 0
        self.conn._on_tune(345, 16, 10)
        assert self.conn.heartbeat == 0

    def test_on_tune_sent(self):
        self.conn._on_tune_sent()
        self.conn.send_method.assert_called_with(
            spec.Connection.Open, 'ssb', (self.conn.virtual_host, '', False),
        )

    def test_on_open_ok(self):
        self.conn.on_open = Mock(name='on_open')
        self.conn._on_open_ok()
        assert self.conn._handshake_complete
        self.conn.on_open.assert_called_with(self.conn)

    def test_connected(self):
        self.conn.transport.connected = False
        assert not self.conn.connected
        self.conn.transport.connected = True
        assert self.conn.connected
        self.conn.transport = None
        assert not self.conn.connected

    def test_collect(self):
        channels = self.conn.channels = {
            0: self.conn, 1: Mock(name='c1'), 2: Mock(name='c2'),
        }
        transport = self.conn.transport
        self.conn.collect()
        transport.close.assert_called_with()
        for i, channel in channels.items():
            if i:
                channel.collect.assert_called_with()
        assert self.conn._transport is None

    def test_collect__transport_socket_raises_os_error(self):
        self.conn.transport = TCPTransport('localhost:5672')
        sock = self.conn.transport.sock = Mock(name='sock')
        channel = Mock(name='c1')
        self.conn.channels = {1: channel}
        sock.shutdown.side_effect = OSError
        self.conn.collect()
        channel.collect.assert_called_with()
        sock.close.assert_called_with()
        assert self.conn._transport is None
        assert self.conn.channels is None

    def test_collect_no_transport(self):
        self.conn = Connection()
        self.conn.connect = Mock(name='connect')
        assert not self.conn.connected
        self.conn.collect()
        assert not self.conn.connect.called

    def test_collect_again(self):
        self.conn = Connection()
        self.conn.collect()
        self.conn.collect()

    def test_get_free_channel_id(self):
        assert self.conn._get_free_channel_id() == 1
        assert self.conn._get_free_channel_id() == 2

    def test_get_free_channel_id__raises_ResourceError(self):
        self.conn.channel_max = 2
        self.conn._get_free_channel_id()
        self.conn._get_free_channel_id()
        with pytest.raises(ResourceError):
            self.conn._get_free_channel_id()

    def test_claim_channel_id(self):
        self.conn._claim_channel_id(30)
        with pytest.raises(ConnectionError):
            self.conn._claim_channel_id(30)

    def test_channel(self):
        callback = Mock(name='callback')
        c = self.conn.channel(3, callback)
        self.conn.Channel.assert_called_with(self.conn, 3, on_open=callback)
        c2 = self.conn.channel(3, callback)
        assert c2 is c

    def test_channel_when_connection_is_closed(self):
        self.conn.collect()
        callback = Mock(name='callback')
        with pytest.raises(RecoverableConnectionError):
            self.conn.channel(3, callback)

    def test_is_alive(self):
        with pytest.raises(NotImplementedError):
            self.conn.is_alive()

    def test_drain_events(self):
        self.conn.blocking_read = Mock(name='blocking_read')
        self.conn.drain_events(30)
        self.conn.blocking_read.assert_called_with(30)

    def test_blocking_read__no_timeout(self):
        self.conn.on_inbound_frame = Mock(name='on_inbound_frame')
        self.conn.transport.having_timeout = ContextMock()
        ret = self.conn.blocking_read(None)
        self.conn.transport.read_frame.assert_called_with()
        self.conn.on_inbound_frame.assert_called_with(
            self.conn.transport.read_frame(),
        )
        assert ret is self.conn.on_inbound_frame()

    def test_blocking_read__timeout(self):
        self.conn.transport = TCPTransport('localhost:5672')
        sock = self.conn.transport.sock = Mock(name='sock')
        sock.gettimeout.return_value = 1
        self.conn.transport.read_frame = Mock(name='read_frame')
        self.conn.on_inbound_frame = Mock(name='on_inbound_frame')
        self.conn.blocking_read(3)
        sock.gettimeout.assert_called_with()
        sock.settimeout.assert_has_calls([call(3), call(1)])
        self.conn.transport.read_frame.assert_called_with()
        self.conn.on_inbound_frame.assert_called_with(
            self.conn.transport.read_frame(),
        )
        sock.gettimeout.return_value = 3
        self.conn.blocking_read(3)

    def test_blocking_read__SSLError(self):
        self.conn.on_inbound_frame = Mock(name='on_inbound_frame')
        self.conn.transport = TCPTransport('localhost:5672')
        sock = self.conn.transport.sock = Mock(name='sock')
        sock.gettimeout.return_value = 1
        self.conn.transport.read_frame = Mock(name='read_frame')
        self.conn.transport.read_frame.side_effect = SSLError(
            'operation timed out')
        with pytest.raises(socket.timeout):
            self.conn.blocking_read(3)
        self.conn.transport.read_frame.side_effect = SSLError(
            'The operation did not complete foo bar')
        with pytest.raises(socket.timeout):
            self.conn.blocking_read(3)
        self.conn.transport.read_frame.side_effect = SSLError(
            'oh noes')
        with pytest.raises(SSLError):
            self.conn.blocking_read(3)

    def test_on_inbound_method(self):
        self.conn.channels[1] = self.conn.channel(1)
        self.conn.on_inbound_method(1, (50, 60), 'payload', 'content')
        self.conn.channels[1].dispatch_method.assert_called_with(
            (50, 60), 'payload', 'content',
        )

    def test_on_inbound_method_when_connection_is_closed(self):
        self.conn.collect()
        with pytest.raises(RecoverableConnectionError):
            self.conn.on_inbound_method(1, (50, 60), 'payload', 'content')

    def test_close(self):
        self.conn.collect = Mock(name='collect')
        self.conn.close(reply_text='foo', method_sig=spec.Channel.Open)
        self.conn.send_method.assert_called_with(
            spec.Connection.Close, 'BsBB',
            (0, 'foo', spec.Channel.Open[0], spec.Channel.Open[1]),
            wait=spec.Connection.CloseOk,
        )

    def test_close__already_closed(self):
        self.conn.transport = None
        self.conn.close()

    def test_close__socket_error(self):
        self.conn.send_method = Mock(name='send_method',
                                     side_effect=socket.error)
        with pytest.raises(socket.error):
            self.conn.close()
        self.conn.send_method.assert_called()
        assert self.conn._transport is None and not self.conn.connected

    def test_on_close(self):
        self.conn._x_close_ok = Mock(name='_x_close_ok')
        with pytest.raises(NotFound):
            self.conn._on_close(404, 'bah not found', 50, 60)

    def test_x_close_ok(self):
        self.conn._x_close_ok()
        self.conn.send_method.assert_called_with(
            spec.Connection.CloseOk, callback=self.conn._on_close_ok,
        )

    def test_on_close_ok(self):
        self.conn.collect = Mock(name='collect')
        self.conn._on_close_ok()
        self.conn.collect.assert_called_with()

    def test_on_blocked(self):
        self.conn._on_blocked()
        self.conn.on_blocked = Mock(name='on_blocked')
        self.conn._on_blocked()
        self.conn.on_blocked.assert_called_with(
            'connection blocked, see broker logs')

    def test_on_unblocked(self):
        self.conn._on_unblocked()
        self.conn.on_unblocked = Mock(name='on_unblocked')
        self.conn._on_unblocked()
        self.conn.on_unblocked.assert_called_with()

    def test_send_heartbeat(self):
        self.conn.send_heartbeat()
        self.conn.frame_writer.assert_called_with(
            8, 0, None, None, None,
        )

    def test_heartbeat_tick__no_heartbeat(self):
        self.conn.heartbeat = 0
        self.conn.heartbeat_tick()

    def test_heartbeat_tick(self):
        self.conn.heartbeat = 3
        self.conn.heartbeat_tick()
        self.conn.bytes_sent = 3124
        self.conn.bytes_recv = 123
        self.conn.heartbeat_tick()
        self.conn.last_heartbeat_received -= 1000
        self.conn.last_heartbeat_sent -= 1000
        with pytest.raises(ConnectionError):
            self.conn.heartbeat_tick()

    def _test_heartbeat_rate_tick(self, rate):
        # Doing 22 calls,
        # First one is setting the variables
        # All nexts may send heartbeats, depending on rate
        for i in range(1, 22):
            self.conn.heartbeat_tick(rate)
            time.sleep(0.1)

    def test_heartbeat_check_rate_default(self):
        # Heartbeat set to 2 secs
        self.conn.heartbeat = 2
        # Default rate is 2 --> should send frames every sec
        self._test_heartbeat_rate_tick(2)
        # Verify that we wrote 2 frames
        assert self.conn.frame_writer.call_count == 2

    def test_heartbeat_check_rate_four(self):
        # Heartbeat set to 2 secs
        self.conn.heartbeat = 2
        # Rate 4 --> should send frames every 0.5sec
        self._test_heartbeat_rate_tick(4)
        # Verify that we wrote 4 frames
        assert self.conn.frame_writer.call_count == 4

    def test_heartbeat_check_rate_wrong(self):
        # Heartbeat set to 2 secs
        self.conn.heartbeat = 2
        # Default rate is 2 --> should send frames every sec
        self._test_heartbeat_rate_tick(-42)
        # Verify that we wrote 2 frames
        assert self.conn.frame_writer.call_count == 2

    def test_server_capabilities(self):
        self.conn.server_properties['capabilities'] = {'foo': 1}
        assert self.conn.server_capabilities == {'foo': 1}

    @pytest.mark.parametrize(
        'conn_kwargs,expected_vhost', [
            ({}, '/'),
            ({'user_id': 'test_user', 'password': 'test_pass'}, '/'),
            ({'virtual_host': 'test_vhost'}, 'test_vhost')
        ]
    )
    def test_repr_disconnected(self, conn_kwargs, expected_vhost):
        assert re.fullmatch(
            r'<AMQP Connection: broker.com:1234/{} '
            r'\(disconnected\) at 0x.*>'.format(expected_vhost),
            repr(Connection(host='broker.com:1234', **conn_kwargs))
        )

    @pytest.mark.parametrize(
        'conn_kwargs,expected_vhost', [
            ({}, '/'),
            ({'user_id': 'test_user', 'password': 'test_pass'}, '/'),
            ({'virtual_host': 'test_vhost'}, 'test_vhost')
        ]
    )
    def test_repr_connected(self, conn_kwargs, expected_vhost):
        c = Connection(host='broker.com:1234', **conn_kwargs)
        c._transport = Mock(name='transport')
        assert re.fullmatch(
            r'<AMQP Connection: broker.com:1234/{} using {} at 0x.*>'.format(
                expected_vhost,
                repr(c.transport)
            ),
            repr(c)
        )


class ScriptedTransport:
    """Fake transport recording written frames and scripting inbound ones."""

    def __init__(self):
        self.written = []
        self.inbound = deque()
        self.connected = True
        self.on_pop = None

    def write(self, data):
        self.written.append(bytes(data))

    def read_frame(self):
        if not self.inbound:
            raise socket.timeout('no scripted inbound frame')
        frame = self.inbound.popleft()
        if self.on_pop is not None:
            self.on_pop(frame)
        return frame

    @contextmanager
    def having_timeout(self, timeout):
        yield

    def close(self):
        self.connected = False


def parse_frames(chunks):
    """Parse recorded socket writes into (type, channel, payload) frames."""
    buf = b''.join(chunks)
    frames = []
    while buf:
        frame_type, channel, size = unpack_from('>BHI', buf, 0)
        payload = bytes(buf[7:7 + size])
        assert buf[7 + size] == 0xce
        frames.append((frame_type, channel, payload))
        buf = buf[7 + size + 1:]
    return frames


def method_payload(sig, args=b''):
    return pack('>HH', *sig) + args


def method_frame(channel, sig, args=b''):
    return (1, channel, method_payload(sig, args))


class test_ConnectionBlockedPublish:
    """Connection-level publish policy for Connection.Blocked.

    These tests inject Blocked/Unblocked/Blocked into a real Connection
    wired to a recording transport, and assert on the frames actually
    written to the socket.
    """

    def make_conn(self, blocked_capability=True, **kwargs):
        conn = Connection(authentication=AMQPLAIN('foo', 'bar'), **kwargs)
        transport = ScriptedTransport()
        conn.transport = transport
        conn.frame_writer = frame_writer(conn, transport)
        conn.on_inbound_frame = frame_handler(conn, conn.on_inbound_method)
        if blocked_capability:
            conn.client_properties['capabilities'] = {
                'connection.blocked': True,
            }
        return conn, transport

    def open_channel(self, conn, channel_id):
        return Channel(conn, channel_id)

    def inject_method(self, conn, channel, sig, args=b''):
        conn.on_inbound_frame(method_frame(channel, sig, args))

    def publish_frames(self, transport):
        return [f for f in parse_frames(transport.written)
                if f[2][:4] == method_payload(spec.Basic.Publish)]

    def test_invalid_policy_rejected(self):
        with pytest.raises(ValueError):
            Connection(blocked_publish_policy='explode')

    def test_reject_policy_raises_and_writes_nothing(self):
        conn, transport = self.make_conn()
        ch = self.open_channel(conn, 1)

        self.inject_method(conn, 0, spec.Connection.Blocked)
        assert conn._blocked

        with pytest.raises(Blocked):
            ch.basic_publish(Message(body=b'm1'), 'ex', 'rk')
        # not a single frame was written for the rejected publish
        assert transport.written == []

        # heartbeats and close frames still flow while blocked
        conn.send_heartbeat()
        conn.send_method(
            spec.Connection.Close, 'BsBB', (200, 'bye', 0, 0))
        frames = parse_frames(transport.written)
        assert [f[0] for f in frames] == [8, 1]
        assert frames[1][2][:4] == method_payload(spec.Connection.Close)

        # after Unblocked publishing works again
        self.inject_method(conn, 0, spec.Connection.Unblocked)
        assert not conn._blocked
        ch.basic_publish(Message(body=b'm1'), 'ex', 'rk')
        assert len(self.publish_frames(transport)) == 1

    def test_queue_policy_orders_channels_and_control_frames_flow(self):
        conn, transport = self.make_conn(blocked_publish_policy='queue')
        ch1 = self.open_channel(conn, 1)
        ch2 = self.open_channel(conn, 2)
        acks = []
        ch1.events['basic_ack'].add(
            lambda tag, multiple: acks.append((tag, multiple)))

        self.inject_method(conn, 0, spec.Connection.Blocked)

        # interleaved publishes on two channels are queued, not written
        p1 = ch1.basic_publish(Message(body=b'm1'), 'ex', 'rk')
        p2 = ch2.basic_publish(Message(body=b'm2'), 'ex', 'rk')
        p3 = ch1.basic_publish(Message(body=b'm3'), 'ex', 'rk')
        assert transport.written == []
        assert conn._blocked_queue_bytes > 0

        # heartbeats, confirms and close frames keep flowing while blocked
        conn.send_heartbeat()
        self.inject_method(
            conn, 1, spec.Basic.Ack, dumps('Lb', (7, False)))
        conn.send_method(
            spec.Connection.Close, 'BsBB', (200, 'bye', 0, 0))
        assert acks == [(7, False)]
        frames = parse_frames(transport.written)
        assert [f[0] for f in frames] == [8, 1]
        assert frames[1][2][:4] == method_payload(spec.Connection.Close)
        assert self.publish_frames(transport) == []

        # Unblocked: the queue drains in exact acceptance order,
        # preserving each channel's publish sequence.
        transport.written.clear()
        self.inject_method(conn, 0, spec.Connection.Unblocked)
        frames = parse_frames(transport.written)
        bodies = [payload for t, ch, payload in frames if t == 3]
        channels = [ch for t, ch, payload in frames if t == 1]
        assert bodies == [b'm1', b'm2', b'm3']
        assert channels == [1, 2, 1]
        assert all(p.ready for p in (p1, p2, p3))
        assert conn._blocked_queue_bytes == 0

        # a second Blocked stops publishing again...
        self.inject_method(conn, 0, spec.Connection.Blocked)
        transport.written.clear()
        ch1.basic_publish(Message(body=b'm4'), 'ex', 'rk')
        assert transport.written == []

        # ...and the second Unblocked flushes what was queued meanwhile
        self.inject_method(conn, 0, spec.Connection.Unblocked)
        frames = parse_frames(transport.written)
        bodies = [payload for t, ch, payload in frames if t == 3]
        assert bodies == [b'm4']

    def test_drain_stops_when_reblocked_mid_flush(self):
        conn, transport = self.make_conn(blocked_publish_policy='queue')
        ch = self.open_channel(conn, 1)

        self.inject_method(conn, 0, spec.Connection.Blocked)
        ch.basic_publish(Message(body=b'm1'), 'ex', 'rk')
        ch.basic_publish(Message(body=b'm2'), 'ex', 'rk')

        # the broker blocks the connection again while the queue is
        # being flushed: draining must stop immediately
        real_writer = conn.frame_writer

        def reblocking_writer(type_, channel, sig, args, content):
            real_writer(type_, channel, sig, args, content)
            conn._blocked = True
        conn.frame_writer = reblocking_writer

        self.inject_method(conn, 0, spec.Connection.Unblocked)
        bodies = [payload for t, c, payload in parse_frames(
            transport.written) if t == 3]
        assert bodies == [b'm1']
        assert len(conn._blocked_queue) == 1

        # the remainder is flushed by the next Unblocked
        conn.frame_writer = real_writer
        self.inject_method(conn, 0, spec.Connection.Unblocked)
        bodies = [payload for t, c, payload in parse_frames(
            transport.written) if t == 3]
        assert bodies == [b'm1', b'm2']
        assert not conn._blocked_queue

    def test_queue_budget_exceeded_rejects_without_fake_confirm(self):
        conn, transport = self.make_conn(
            blocked_publish_policy='queue',
            blocked_queue_max_bytes=128,
            confirm_publish=True,
        )
        ch = self.open_channel(conn, 1)
        ch._confirm_selected = True
        ch.wait = Mock(name='wait')

        self.inject_method(conn, 0, spec.Connection.Blocked)

        # first publish fits the byte budget and is queued
        ch.basic_publish(Message(body=b'x' * 32), 'ex', 'rk')
        assert conn._blocked_queue_bytes > 0
        ch.wait.reset_mock()

        # second publish exceeds the budget: rejected with Blocked,
        # and no confirm receipt is fabricated for it
        with pytest.raises(Blocked):
            ch.basic_publish(Message(body=b'x' * 32), 'ex', 'rk')
        ch.wait.assert_not_called()
        assert transport.written == []

        # only the first (queued) publish is written after Unblocked
        self.inject_method(conn, 0, spec.Connection.Unblocked)
        bodies = [payload for t, ch_, payload in parse_frames(
            transport.written) if t == 3]
        assert bodies == [b'x' * 32]

    def test_confirm_publish_waits_through_blocked_period(self):
        conn, transport = self.make_conn(
            blocked_capability=False,
            blocked_publish_policy='queue',
        )
        ch = self.open_channel(conn, 1)
        ch._confirm_selected = True

        self.inject_method(conn, 0, spec.Connection.Blocked)

        # broker unblocks, then confirms delivery tag 1
        transport.inbound.append(method_frame(0, spec.Connection.Unblocked))
        transport.inbound.append(
            method_frame(1, spec.Basic.Ack, dumps('Lb', (1, False))))

        # nothing may be written before the Unblocked frame is read
        written_at_unblock = []

        def on_pop(frame):
            if frame[2][:4] == method_payload(spec.Connection.Unblocked):
                written_at_unblock.extend(transport.written)
        transport.on_pop = on_pop

        ch.basic_publish_confirm(Message(body=b'hello'), 'ex', 'rk')

        assert written_at_unblock == []
        bodies = [payload for t, ch_, payload in parse_frames(
            transport.written) if t == 3]
        assert bodies == [b'hello']

    def test_close_fails_queued_publishes(self):
        conn, transport = self.make_conn(blocked_publish_policy='queue')
        ch = self.open_channel(conn, 1)

        self.inject_method(conn, 0, spec.Connection.Blocked)
        p1 = ch.basic_publish(Message(body=b'm1'), 'ex', 'rk')
        p2 = ch.basic_publish(Message(body=b'm2'), 'ex', 'rk')

        conn.collect()

        # queued publishes are handed back as explicitly failed
        for p in (p1, p2):
            assert p.failed
            assert isinstance(p.reason, RecoverableConnectionError)
        assert not conn._blocked_queue
        assert conn._blocked_queue_bytes == 0
        assert transport.written == []

    def test_reconnect_fails_leftover_queue_instead_of_replaying(self):
        conn, transport = self.make_conn(blocked_publish_policy='queue')
        ch = self.open_channel(conn, 1)

        self.inject_method(conn, 0, spec.Connection.Blocked)
        p = ch.basic_publish(Message(body=b'm1'), 'ex', 'rk')

        # the old transport is gone; a reconnect must fail the queued
        # publish, never silently republish it on the new connection
        transport.connected = False
        new_transport = Mock(name='new_transport')
        new_transport.having_timeout = ContextMock()
        new_transport.read_frame.side_effect = OSError('no broker')
        conn.Transport = Mock(name='Transport', return_value=new_transport)
        with pytest.raises(OSError):
            conn.connect()

        assert p.failed
        assert isinstance(p.reason, RecoverableConnectionError)
        assert not conn._blocked
        new_transport.write.assert_not_called()

    def test_unblocked_with_empty_queue(self):
        conn, transport = self.make_conn(blocked_publish_policy='queue')
        conn.on_unblocked = Mock(name='on_unblocked')
        self.inject_method(conn, 0, spec.Connection.Blocked)
        self.inject_method(conn, 0, spec.Connection.Unblocked)
        conn.on_unblocked.assert_called_once_with()
        assert transport.written == []
