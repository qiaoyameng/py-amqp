import socket
from struct import pack
from unittest.mock import ANY, MagicMock, Mock, call, patch

import pytest
from vine import promise

from amqp import spec
from amqp.basic_message import Message
from amqp.channel import Channel
from amqp.exceptions import (ConsumerCancelled, MessageNacked, NotFound,
                             RecoverableConnectionError)
from amqp.serialization import dumps

from t.mocks import ContextMock

class test_Channel:

    @pytest.fixture(autouse=True)
    def setup_conn(self):
        self.conn = MagicMock(name='connection')
        self.conn.is_closing = False
        self.conn.channels = {}
        self.conn._get_free_channel_id.return_value = 2
        self.c = Channel(self.conn, 1)
        self.c.send_method = Mock(name='send_method')

    def test_init_confirm_enabled(self):
        self.conn.confirm_publish = True
        c = Channel(self.conn, 2)
        assert c.basic_publish == c.basic_publish_confirm

    def test_init_confirm_disabled(self):
        self.conn.confirm_publish = False
        c = Channel(self.conn, 2)
        assert c.basic_publish == c._basic_publish

    def test_init_auto_channel(self):
        c = Channel(self.conn, None)
        self.conn._get_free_channel_id.assert_called_with()
        assert c.channel_id is self.conn._get_free_channel_id()

    def test_init_explicit_channel(self):
        Channel(self.conn, 3)
        self.conn._claim_channel_id.assert_called_with(3)

    def test_then(self):
        self.c.on_open = Mock(name='on_open')
        on_success = Mock(name='on_success')
        on_error = Mock(name='on_error')
        self.c.then(on_success, on_error)
        self.c.on_open.then.assert_called_with(on_success, on_error)

    def test_collect(self):
        self.c.callbacks[(50, 61)] = Mock()
        self.c.cancel_callbacks['foo'] = Mock()
        self.c.events['bar'].add(Mock())
        self.c.no_ack_consumers.add('foo')
        self.c.collect()
        assert not self.c.callbacks
        assert not self.c.cancel_callbacks
        assert not self.c.events
        assert not self.c.no_ack_consumers
        assert not self.c.is_open
        self.c.collect()

    def test_do_revive(self):
        self.c.open = Mock(name='open')
        self.c.is_open = True
        self.c._do_revive()
        assert not self.c.is_open
        self.c.open.assert_called_with()

    def test_close__not_open(self):
        self.c.is_open = False
        self.c.close()

    def test_close__no_connection(self):
        self.c.connection = None
        self.c.close()

    def test_close(self):
        self.c.is_open = True
        self.c.close(30, 'text', spec.Queue.Declare)
        self.c.send_method.assert_called_with(
            spec.Channel.Close, 'BsBB',
            (30, 'text', spec.Queue.Declare[0], spec.Queue.Declare[1]),
            wait=spec.Channel.CloseOk,
        )
        assert self.c.is_closing is False
        assert self.c.connection is None

    @pytest.mark.parametrize(
        "channel_is_closing, connection_is_closing, expect_error",
        [(False, False, True), (True, False, False), (False, True, False)],
    )
    def test_on_close(self, channel_is_closing, connection_is_closing, expect_error):
        self.c._do_revive = Mock(name='_do_revive')
        self.c.is_closing = channel_is_closing
        self.conn.is_closing = connection_is_closing
        if expect_error:
            with pytest.raises(NotFound):
                self.c._on_close(404, 'text', 50, 61)
            self.c._do_revive.assert_called_with()
        else:
            self.c._on_close(404, 'text', 50, 61)
            self.c._do_revive.assert_not_called()
        self.c.send_method.assert_called_with(spec.Channel.CloseOk)

    def test_on_close_ok(self):
        self.c.collect = Mock(name='collect')
        self.c._on_close_ok()
        self.c.collect.assert_called_with()

    def test_flow(self):
        self.c.flow(0)
        self.c.send_method.assert_called_with(
            spec.Channel.Flow, 'b', (0,), wait=spec.Channel.FlowOk,
        )

    def test_on_flow(self):
        self.c._x_flow_ok = Mock(name='_x_flow_ok')
        self.c._on_flow(0)
        assert not self.c.active
        self.c._x_flow_ok.assert_called_with(0)

    def test_x_flow_ok(self):
        self.c._x_flow_ok(1)
        self.c.send_method.assert_called_with(spec.Channel.FlowOk, 'b', (1,))

    def test_open(self):
        self.c.is_open = True
        self.c.open()
        self.c.is_open = False
        self.c.open()
        self.c.send_method.assert_called_with(
            spec.Channel.Open, 's', ('',), wait=spec.Channel.OpenOk,
        )

    def test_on_open_ok(self):
        self.c.on_open = Mock(name='on_open')
        self.c.is_open = False
        self.c._on_open_ok()
        assert self.c.is_open
        self.c.on_open.assert_called_with(self.c)

    def test_exchange_declare(self):
        self.c.exchange_declare(
            'foo', 'direct', False, True,
            auto_delete=False, nowait=False, arguments={'x': 1},
        )
        self.c.send_method.assert_called_with(
            spec.Exchange.Declare, 'BssbbbbbF',
            (0, 'foo', 'direct', False, True, False,
                False, False, {'x': 1}),
            wait=spec.Exchange.DeclareOk,
        )

    def test_exchange_declare__auto_delete(self):
        self.c.exchange_declare(
            'foo', 'direct', False, True,
            auto_delete=True, nowait=False, arguments={'x': 1},
        )

    def test_exchange_delete(self):
        self.c.exchange_delete('foo')
        self.c.send_method.assert_called_with(
            spec.Exchange.Delete, 'Bsbb',
            (0, 'foo', False, False),
            wait=spec.Exchange.DeleteOk,
        )

    def test_exchange_bind(self):
        self.c.exchange_bind('dest', 'source', 'rkey', arguments={'x': 1})
        self.c.send_method.assert_called_with(
            spec.Exchange.Bind, 'BsssbF',
            (0, 'dest', 'source', 'rkey', False, {'x': 1}),
            wait=spec.Exchange.BindOk,
        )

    def test_exchange_unbind(self):
        self.c.exchange_unbind('dest', 'source', 'rkey', arguments={'x': 1})
        self.c.send_method.assert_called_with(
            spec.Exchange.Unbind, 'BsssbF',
            (0, 'dest', 'source', 'rkey', False, {'x': 1}),
            wait=spec.Exchange.UnbindOk,
        )

    def test_queue_bind(self):
        self.c.queue_bind('q', 'ex', 'rkey', arguments={'x': 1})
        self.c.send_method.assert_called_with(
            spec.Queue.Bind, 'BsssbF',
            (0, 'q', 'ex', 'rkey', False, {'x': 1}),
            wait=spec.Queue.BindOk,
        )

    def test_queue_unbind(self):
        self.c.queue_unbind('q', 'ex', 'rkey', arguments={'x': 1})
        self.c.send_method.assert_called_with(
            spec.Queue.Unbind, 'BsssF',
            (0, 'q', 'ex', 'rkey', {'x': 1}),
            wait=spec.Queue.UnbindOk,
        )

    def test_queue_declare(self):
        self.c.queue_declare('q', False, True, False, False, True, {'x': 1})
        self.c.send_method.assert_called_with(
            spec.Queue.Declare, 'BsbbbbbF',
            (0, 'q', False, True, False, False, True, {'x': 1}),
        )

    def test_queue_declare__sync(self):
        self.c.wait = Mock(name='wait')
        self.c.wait.return_value = ('name', 123, 45)
        ret = self.c.queue_declare(
            'q', False, True, False, False, False, {'x': 1},
        )
        self.c.send_method.assert_called_with(
            spec.Queue.Declare, 'BsbbbbbF',
            (0, 'q', False, True, False, False, False, {'x': 1}),
        )
        assert ret.queue == 'name'
        assert ret.message_count == 123
        assert ret.consumer_count == 45
        self.c.wait.assert_called_with(
            spec.Queue.DeclareOk, returns_tuple=True)

    def test_queue_delete(self):
        self.c.queue_delete('q')
        self.c.send_method.assert_called_with(
            spec.Queue.Delete, 'Bsbbb',
            (0, 'q', False, False, False),
            wait=spec.Queue.DeleteOk,
        )

    def test_queue_purge(self):
        self.c.queue_purge('q')
        self.c.send_method.assert_called_with(
            spec.Queue.Purge, 'Bsb', (0, 'q', False),
            wait=spec.Queue.PurgeOk,
        )

    def test_basic_ack(self):
        self.c.basic_ack(123, multiple=1)
        self.c.send_method.assert_called_with(
            spec.Basic.Ack, 'Lb', (123, 1),
        )

    def test_basic_cancel(self):
        self.c.basic_cancel(123)
        self.c.send_method.assert_called_with(
            spec.Basic.Cancel, 'sb', (123, False),
            wait=spec.Basic.CancelOk,
        )
        self.c.connection = None
        self.c.basic_cancel(123)

    def test_on_basic_cancel(self):
        self.c._remove_tag = Mock(name='_remove_tag')
        self.c._on_basic_cancel(123)
        self.c._remove_tag.return_value.assert_called_with(123)
        self.c._remove_tag.return_value = None
        with pytest.raises(ConsumerCancelled):
            self.c._on_basic_cancel(123)

    def test_on_basic_cancel_ok(self):
        self.c._remove_tag = Mock(name='remove_tag')
        self.c._on_basic_cancel_ok(123)
        self.c._remove_tag.assert_called_with(123)

    def test_remove_tag(self):
        self.c.callbacks[123] = Mock()
        p = self.c.cancel_callbacks[123] = Mock()
        assert self.c._remove_tag(123) is p
        assert 123 not in self.c.callbacks
        assert 123 not in self.c.cancel_callbacks

    def test_basic_consume(self):
        callback = Mock()
        on_cancel = Mock()
        self.c.send_method.return_value = (123, )
        self.c.basic_consume(
            'q', 123, arguments={'x': 1},
            callback=callback,
            on_cancel=on_cancel,
        )
        self.c.send_method.assert_called_with(
            spec.Basic.Consume, 'BssbbbbF',
            (0, 'q', 123, False, False, False, False, {'x': 1}),
            wait=spec.Basic.ConsumeOk,
            returns_tuple=True
        )
        assert self.c.callbacks[123] is callback
        assert self.c.cancel_callbacks[123] is on_cancel

    def test_basic_consume__no_ack(self):
        self.c.send_method.return_value = (123,)
        self.c.basic_consume(
            'q', 123, arguments={'x': 1}, no_ack=True,
        )
        assert 123 in self.c.no_ack_consumers

    def test_basic_consume_no_consumer_tag(self):
        callback = Mock()
        self.c.send_method.return_value = (123,)
        ret = self.c.basic_consume(
            'q', arguments={'x': 1},
            callback=callback,
        )
        self.c.send_method.assert_called_with(
            spec.Basic.Consume, 'BssbbbbF',
            (0, 'q', '', False, False, False, False, {'x': 1}),
            wait=spec.Basic.ConsumeOk,
            returns_tuple=True
        )
        assert self.c.callbacks[123] is callback
        assert ret == 123

    def test_basic_consume_no_wait(self):
        callback = Mock()
        ret_promise = promise()
        self.c.send_method.return_value = ret_promise
        ret = self.c.basic_consume(
            'q', 123, arguments={'x': 1},
            callback=callback, nowait=True
        )
        self.c.send_method.assert_called_with(
            spec.Basic.Consume, 'BssbbbbF',
            (0, 'q', 123, False, False, False, True, {'x': 1}),
            wait=None,
            returns_tuple=True
        )
        assert self.c.callbacks[123] is callback
        assert ret == ret_promise

    def test_basic_consume_no_wait_no_consumer_tag(self):
        callback = Mock()
        with pytest.raises(ValueError):
            self.c.basic_consume(
                'q', arguments={'x': 1},
                callback=callback, nowait=True
            )
        assert 123 not in self.c.callbacks

    def test_on_basic_deliver(self):
        msg = Message()
        self.c._on_basic_deliver(123, '321', False, 'ex', 'rkey', msg)
        callback = self.c.callbacks[123] = Mock(name='cb')

        self.c._on_basic_deliver(123, '321', False, 'ex', 'rkey', msg)
        callback.assert_called_with(msg)
        assert msg.channel == self.c
        assert msg.delivery_info == {
            'consumer_tag': 123,
            'delivery_tag': '321',
            'redelivered': False,
            'exchange': 'ex',
            'routing_key': 'rkey',
        }

    def test_delivery_handle_coalesces_completed_prefix(self):
        tracked = Channel(self.conn, 4, delivery_handles=True)
        tracked.send_method = Mock(name='send_method')
        tracked.callbacks['consumer'] = Mock()
        messages = [Message(), Message(), Message()]

        for index, message in enumerate(messages, 1):
            tracked._on_basic_deliver(
                'consumer', index, False, 'ex', 'rkey', message
            )

        messages[1].delivery_handle.ack()
        messages[2].delivery_handle.ack()
        tracked.send_method.assert_not_called()

        messages[0].delivery_handle.ack()
        tracked.send_method.assert_called_once_with(
            spec.Basic.Ack, 'Lb', (3, True),
        )

    def test_basic_ack_tracks_handles_without_resending_prefix(self):
        tracked = Channel(self.conn, 4, delivery_handles=True)
        tracked.send_method = Mock(name='send_method')
        tracked.callbacks['consumer'] = Mock()
        messages = [Message(), Message(), Message()]

        for index, message in enumerate(messages, 1):
            tracked._on_basic_deliver(
                'consumer', index, False, 'ex', 'rkey', message
            )

        tracked.basic_ack(2, multiple=True)

        assert messages[0].delivery_handle.settled
        assert messages[1].delivery_handle.settled
        assert not messages[2].delivery_handle.settled
        tracked.send_method.assert_called_once_with(
            spec.Basic.Ack, 'Lb', (2, True),
        )

        messages[2].delivery_handle.ack()
        tracked.send_method.assert_called_with(
            spec.Basic.Ack, 'Lb', (3, False),
        )

    def test_delivery_handle_sends_completed_prefix_before_pending(self):
        tracked = Channel(self.conn, 4, delivery_handles=True)
        tracked.send_method = Mock(name='send_method')
        tracked.callbacks['consumer'] = Mock()
        messages = [Message(), Message(), Message()]

        for index, message in enumerate(messages, 1):
            tracked._on_basic_deliver(
                'consumer', index, False, 'ex', 'rkey', message
            )

        messages[0].delivery_handle.ack()
        messages[1].delivery_handle.ack()
        tracked.send_method.assert_called_once_with(
            spec.Basic.Ack, 'Lb', (2, True),
        )

    def test_flush_delivery_acks_does_not_cross_pending_message(self):
        tracked = Channel(self.conn, 4, delivery_handles=True)
        tracked.send_method = Mock(name='send_method')
        tracked.callbacks['consumer'] = Mock()
        messages = [Message(), Message(), Message()]

        for index, message in enumerate(messages, 1):
            tracked._on_basic_deliver(
                'consumer', index, False, 'ex', 'rkey', message
            )

        messages[0].delivery_handle.ack()
        messages[2].delivery_handle.ack()
        tracked.send_method.assert_not_called()

        tracked.flush_delivery_acks()
        tracked.send_method.assert_called_once_with(
            spec.Basic.Ack, 'Lb', (1, False),
        )

    def test_delivery_handle_reject_prevents_multiple_ack_across_it(self):
        tracked = Channel(self.conn, 4, delivery_handles=True)
        tracked.send_method = Mock(name='send_method')
        tracked.callbacks['consumer'] = Mock()
        messages = [Message(), Message(), Message()]

        for index, message in enumerate(messages, 1):
            tracked._on_basic_deliver(
                'consumer', index, False, 'ex', 'rkey', message
            )

        messages[0].delivery_handle.reject()
        messages[1].delivery_handle.ack()
        messages[2].delivery_handle.ack()

        assert tracked.send_method.call_args_list == [
            call(spec.Basic.Reject, 'Lb', (1, True)),
            call(spec.Basic.Ack, 'Lb', (2, False)),
            call(spec.Basic.Ack, 'Lb', (3, False)),
        ]

    def test_delivery_handle_nack_sends_nack_frame(self):
        tracked = Channel(self.conn, 4, delivery_handles=True)
        tracked.send_method = Mock(name='send_method')
        tracked.callbacks['consumer'] = Mock()
        message = Message()
        tracked._on_basic_deliver(
            'consumer', 5, False, 'ex', 'rkey', message
        )

        message.delivery_handle.nack(requeue=True)

        tracked.send_method.assert_called_once_with(
            spec.Basic.Nack, 'Lbb', (5, False, True),
        )

    def test_delivery_handle_can_only_settle_once(self):
        tracked = Channel(self.conn, 4, delivery_handles=True)
        tracked.send_method = Mock(name='send_method')
        tracked.callbacks['consumer'] = Mock()
        message = Message()
        tracked._on_basic_deliver(
            'consumer', 5, False, 'ex', 'rkey', message
        )

        message.delivery_handle.ack()
        with pytest.raises(RuntimeError):
            message.delivery_handle.nack()

    def test_delivery_handles_are_invalidated_when_channel_collects(self):
        tracked = Channel(self.conn, 4, delivery_handles=True)
        tracked.send_method = Mock(name='send_method')
        tracked.callbacks['consumer'] = Mock()
        message = Message()
        tracked._on_basic_deliver(
            'consumer', 5, False, 'ex', 'rkey', message
        )

        tracked.collect()

        with pytest.raises(RecoverableConnectionError):
            message.delivery_handle.ack()
        tracked.send_method.assert_not_called()

    def test_delivery_handles_are_invalidated_when_channel_closes(self):
        tracked = Channel(self.conn, 4, delivery_handles=True)
        tracked.send_method = Mock(name='send_method')
        tracked.is_open = True
        tracked.callbacks['consumer'] = Mock()
        message = Message()
        tracked._on_basic_deliver(
            'consumer', 5, False, 'ex', 'rkey', message
        )

        tracked.close()

        with pytest.raises(RecoverableConnectionError):
            message.delivery_handle.ack()
        tracked.send_method.assert_called_once_with(
            spec.Channel.Close, 'BsBB',
            (0, '', 0, 0),
            wait=spec.Channel.CloseOk,
        )

    def test_delivery_handle_not_created_for_no_ack_consumer(self):
        tracked = Channel(self.conn, 4, delivery_handles=True)
        tracked.send_method = Mock(name='send_method')
        tracked.no_ack_consumers.add('consumer')
        tracked.callbacks['consumer'] = Mock()
        message = Message()

        tracked._on_basic_deliver(
            'consumer', 5, True, 'ex', 'rkey', message
        )

        assert message.delivery_handle is None
        tracked.send_method.assert_not_called()

    def test_basic_consume_delivery_handle_requires_ack_mode(self):
        with pytest.raises(ValueError):
            self.c.basic_consume(
                'q', 'consumer', no_ack=True, delivery_handle=True
            )
        self.c.send_method.assert_not_called()

    def test_basic_consume_delivery_handle(self):
        self.c.send_method.return_value = ('consumer',)
        callback = Mock()
        self.c.basic_consume(
            'q', 'consumer', delivery_handle=True, callback=callback
        )
        assert 'consumer' in self.c._delivery_handle_consumers

        message = Message()
        self.c._on_basic_deliver(
            'consumer', 7, False, 'ex', 'rkey', message
        )

        message.delivery_handle.ack()
        self.c.send_method.assert_called_with(
            spec.Basic.Ack, 'Lb', (7, False),
        )

    def test_basic_get_delivery_handle(self):
        tracked = Channel(self.conn, 4)
        tracked.send_method = Mock(name='send_method')
        tracked.send_method.return_value = (
            7, False, 'ex', 'rkey', 0, Message()
        )

        message = tracked.basic_get('q', delivery_handle=True)

        assert message.delivery_handle is not None
        message.delivery_handle.ack()
        tracked.send_method.assert_called_with(
            spec.Basic.Ack, 'Lb', (7, False),
        )

    def test_basic_get_delivery_handle_requires_ack_mode(self):
        with pytest.raises(ValueError):
            self.c.basic_get('q', no_ack=True, delivery_handle=True)
        self.c.send_method.assert_not_called()

    def test_redelivered_message_gets_new_delivery_identity(self):
        tracked = Channel(self.conn, 4, delivery_handles=True)
        tracked.send_method = Mock(name='send_method')
        tracked.callbacks['consumer'] = Mock()
        first = Message()
        second = Message()

        tracked._on_basic_deliver(
            'consumer', 9, False, 'ex', 'rkey', first
        )
        tracked._on_basic_deliver(
            'consumer', 9, True, 'ex', 'rkey', second
        )

        assert first.delivery_handle is not second.delivery_handle
        with pytest.raises(RecoverableConnectionError):
            first.delivery_handle.ack()
        second.delivery_handle.ack()
        tracked.send_method.assert_called_once_with(
            spec.Basic.Ack, 'Lb', (9, False),
        )

    def test_basic_get(self):
        self.c._on_get_empty = Mock()
        self.c._on_get_ok = Mock()
        self.c.send_method.return_value = ('cluster_id',)
        self.c.basic_get('q')
        self.c.send_method.assert_called_with(
            spec.Basic.Get, 'Bsb', (0, 'q', False),
            wait=[spec.Basic.GetOk, spec.Basic.GetEmpty], returns_tuple=True,
        )
        self.c._on_get_empty.assert_called_with('cluster_id')
        self.c.send_method.return_value = (
            'dtag', 'redelivered', 'ex', 'rkey', 'mcount', 'msg',
        )
        self.c.basic_get('q')
        self.c._on_get_ok.assert_called_with(
            'dtag', 'redelivered', 'ex', 'rkey', 'mcount', 'msg',
        )

    def test_on_get_empty(self):
        self.c._on_get_empty(1)

    def test_on_get_ok(self):
        msg = Message()
        m = self.c._on_get_ok(
            'dtag', 'redelivered', 'ex', 'rkey', 'mcount', msg,
        )
        assert m is msg
        assert m.channel == self.c
        assert m.delivery_info == {
            'delivery_tag': 'dtag',
            'redelivered': 'redelivered',
            'exchange': 'ex',
            'routing_key': 'rkey',
            'message_count': 'mcount',
        }

    def test_basic_publish(self):
        self.c.connection.transport.having_timeout = ContextMock()
        self.c._basic_publish('msg', 'ex', 'rkey')
        self.c.send_method.assert_called_with(
            spec.Basic.Publish, 'Bssbb',
            (0, 'ex', 'rkey', False, False), 'msg',
        )

    @pytest.mark.parametrize('exc_type', [socket.timeout, TimeoutError])
    def test_basic_publish_timeout_is_connection_error(self, exc_type):
        self.c.connection.transport.having_timeout = ContextMock()
        self.c.send_method.side_effect = exc_type
        with pytest.raises(RecoverableConnectionError, match='timed out'):
            self.c._basic_publish('msg', 'ex', 'rkey')

    def test_basic_publish_confirm(self):
        self.c._confirm_selected = False
        self.c.confirm_select = Mock(name='confirm_select')
        self.c._basic_publish = Mock(name='_basic_publish')
        self.c.wait = Mock(name='wait')
        ret = self.c.basic_publish_confirm(1, 2, arg=1)
        self.c.confirm_select.assert_called_with()
        assert self.c._confirm_selected
        self.c._basic_publish.assert_called_with(1, 2, arg=1)
        assert ret is self.c._basic_publish()
        self.c.wait.assert_called_with(
            [spec.Basic.Ack, spec.Basic.Nack],
            callback=ANY,
            timeout=None
        )
        self.c.basic_publish_confirm(1, 2, arg=1)

    def test_basic_publish_confirm_nack(self):
        # test checking whether library is handling correctly Nack confirms
        # sent from RabbitMQ. Library must raise MessageNacked when server
        # sent Nack message.

        # Nack frame construction
        args = dumps('Lbb', (1, False, False))
        frame = (b''.join([pack('>HH', *spec.Basic.Nack), args]))

        def wait(method, *args, **kwargs):
            # Simple mock simulating registering callbacks of real wait method
            for m in method:
                self.c._pending[m] = kwargs['callback']

        self.c._basic_publish = Mock(name='_basic_publish')
        self.c.wait = Mock(name='wait', side_effect=wait)

        self.c.basic_publish_confirm(1, 2, arg=1)

        with pytest.raises(MessageNacked):
            # Inject Nack to message handler
            self.c.dispatch_method(
                spec.Basic.Nack, frame, None
            )

    def test_basic_publish_connection_blocked(self):
        # Basic test checking that drain_events() is called
        # before publishing message and send_method() is called
        self.c._basic_publish('msg', 'ex', 'rkey')
        self.conn.drain_events.assert_called_once_with(timeout=0)
        self.c.send_method.assert_called_once_with(
            spec.Basic.Publish, 'Bssbb',
            (0, 'ex', 'rkey', False, False), 'msg',
        )

        self.c.send_method.reset_mock()

        # Basic test checking that socket.timeout exception
        # is ignored and send_method() is called.
        self.conn.drain_events.side_effect = socket.timeout
        self.c._basic_publish('msg', 'ex', 'rkey')
        self.c.send_method.assert_called_once_with(
            spec.Basic.Publish, 'Bssbb',
            (0, 'ex', 'rkey', False, False), 'msg',
        )

    def test_basic_publish_connection_blocked_not_supported(self):
        # Test veryfying that when server does not have
        # connection.blocked capability, drain_events() are not called
        self.conn.client_properties = {
            'capabilities': {
                'connection.blocked': False
            }
        }
        self.c._basic_publish('msg', 'ex', 'rkey')
        self.conn.drain_events.assert_not_called()
        self.c.send_method.assert_called_once_with(
            spec.Basic.Publish, 'Bssbb',
            (0, 'ex', 'rkey', False, False), 'msg',
        )

    def test_basic_publish_connection_blocked_not_supported_missing(self):
        # Test veryfying that when server does not have
        # connection.blocked capability, drain_events() are not called
        self.conn.client_properties = {
            'capabilities': {}
        }
        self.c._basic_publish('msg', 'ex', 'rkey')
        self.conn.drain_events.assert_not_called()
        self.c.send_method.assert_called_once_with(
            spec.Basic.Publish, 'Bssbb',
            (0, 'ex', 'rkey', False, False), 'msg',
        )

    def test_basic_publish_connection_blocked_no_capabilities(self):
        # Test veryfying that when server does not have
        # support of capabilities, drain_events() are not called
        self.conn.client_properties = {
        }
        self.c._basic_publish('msg', 'ex', 'rkey')
        self.conn.drain_events.assert_not_called()
        self.c.send_method.assert_called_once_with(
            spec.Basic.Publish, 'Bssbb',
            (0, 'ex', 'rkey', False, False), 'msg',
        )

    def test_basic_publish_confirm_callback(self):

        def wait_nack(method, *args, **kwargs):
            kwargs['callback'](spec.Basic.Nack)

        def wait_ack(method, *args, **kwargs):
            kwargs['callback'](spec.Basic.Ack)

        self.c._basic_publish = Mock(name='_basic_publish')
        self.c.wait = Mock(name='wait_nack', side_effect=wait_nack)

        with pytest.raises(MessageNacked):
            # when callback is called with spec.Basic.Nack it must raise
            # MessageNacked exception
            self.c.basic_publish_confirm(1, 2, arg=1)

        self.c.wait = Mock(name='wait_ack', side_effect=wait_ack)

        # when callback is called with spec.Basic.Ack
        # it must nost raise exception
        self.c.basic_publish_confirm(1, 2, arg=1)

    def test_basic_publish_connection_closed(self):
        self.c.collect()
        with pytest.raises(RecoverableConnectionError) as excinfo:
            self.c._basic_publish('msg', 'ex', 'rkey')
        assert 'basic_publish: connection closed' in str(excinfo.value)
        self.c.send_method.assert_not_called()

    def test_basic_qos(self):
        self.c.basic_qos(0, 123, False)
        self.c.send_method.assert_called_with(
            spec.Basic.Qos, 'lBb', (0, 123, False),
            wait=spec.Basic.QosOk,
        )

    def test_basic_recover(self):
        self.c.basic_recover(requeue=True)
        self.c.send_method.assert_called_with(
            spec.Basic.Recover, 'b', (True,),
        )

    def test_basic_recover_async(self):
        self.c.basic_recover_async(requeue=True)
        self.c.send_method.assert_called_with(
            spec.Basic.RecoverAsync, 'b', (True,),
        )

    def test_basic_reject(self):
        self.c.basic_reject(123, requeue=True)
        self.c.send_method.assert_called_with(
            spec.Basic.Reject, 'Lb', (123, True),
        )

    def test_basic_nack(self):
        self.c.basic_nack(123, multiple=True, requeue=True)
        self.c.send_method.assert_called_with(
            spec.Basic.Nack, 'Lbb', (123, True, True),
        )

    def test_on_basic_return(self):
        with pytest.raises(NotFound):
            self.c._on_basic_return(404, 'text', 'ex', 'rkey', 'msg')

    def test_on_basic_return__handled(self):
        with patch('amqp.channel.error_for_code') as error_for_code:
            callback = Mock(name='callback')
            self.c.events['basic_return'].add(callback)
            self.c._on_basic_return(404, 'text', 'ex', 'rkey', 'msg')
            callback.assert_called_with(
                error_for_code(), 'ex', 'rkey', 'msg',
            )

    def test_tx_commit(self):
        self.c.tx_commit()
        self.c.send_method.assert_called_with(
            spec.Tx.Commit, wait=spec.Tx.CommitOk,
        )

    def test_tx_rollback(self):
        self.c.tx_rollback()
        self.c.send_method.assert_called_with(
            spec.Tx.Rollback, wait=spec.Tx.RollbackOk,
        )

    def test_tx_select(self):
        self.c.tx_select()
        self.c.send_method.assert_called_with(
            spec.Tx.Select, wait=spec.Tx.SelectOk,
        )

    def test_confirm_select(self):
        self.c.confirm_select()
        self.c.send_method.assert_called_with(
            spec.Confirm.Select, 'b', (False,),
            wait=spec.Confirm.SelectOk,
        )

    def test_on_basic_ack(self):
        callback = Mock(name='callback')
        self.c.events['basic_ack'].add(callback)
        self.c._on_basic_ack(123, True)
        callback.assert_called_with(123, True)

    def test_on_basic_nack(self):
        callback = Mock(name='callback')
        self.c.events['basic_nack'].add(callback)
        self.c._on_basic_nack(123, True, False)
        callback.assert_called_with(123, True)
