from unittest.mock import MagicMock, Mock

import pytest

from amqp import spec
from amqp.basic_message import Message
from amqp.channel import Channel
from amqp.delivery import (DeliveryAlreadySettled, DeliveryHandle,
                           DeliveryHandleInvalid)


class test_DeliveryHandleCoalescing:

    @pytest.fixture(autouse=True)
    def setup_channel(self):
        self.conn = MagicMock(name='connection')
        self.conn.is_closing = False
        self.conn.channels = {}
        self.conn._get_free_channel_id.return_value = 2
        self.c = Channel(self.conn, 1)
        self.c.send_method = Mock(name='send_method')
        self.c.send_method.return_value = ('ctag',)
        self.c.basic_consume(
            'q', 'ctag', callback=Mock(), delivery_handle=True,
        )
        self.c.send_method.reset_mock()

    def _deliver(self, delivery_tag, consumer_tag='ctag',
                 redelivered=False):
        msg = Message()
        self.c._on_basic_deliver(
            consumer_tag, delivery_tag, redelivered, 'ex', 'rkey', msg,
        )
        return msg

    def test_handle_attached_to_message(self):
        msg = self._deliver(1)
        assert isinstance(msg.delivery_handle, DeliveryHandle)
        assert msg.delivery_handle.delivery_tag == 1
        assert msg.delivery_handle.consumer_tag == 'ctag'
        assert msg.delivery_info['delivery_handle'] is msg.delivery_handle

    def test_completing_later_tags_does_not_send_ack_early(self):
        h1 = self._deliver(1).delivery_handle
        h2 = self._deliver(2).delivery_handle
        h3 = self._deliver(3).delivery_handle

        h2.ack()
        h3.ack()

        assert self.c.send_method.call_args_list == []

        h1.ack()

        self.c.send_method.assert_called_once_with(
            spec.Basic.Ack, 'Lb', (3, True),
        )

    def test_single_completed_prefix_acks_without_multiple(self):
        self._deliver(1).delivery_handle.ack()
        self.c.send_method.assert_called_once_with(
            spec.Basic.Ack, 'Lb', (1, False),
        )

    def test_gap_blocks_multiple_ack_until_prefix_completes(self):
        h1 = self._deliver(1).delivery_handle
        h2 = self._deliver(2).delivery_handle
        self._deliver(3)

        h2.ack()
        assert self.c.send_method.call_args_list == []

        h1.ack()
        self.c.send_method.assert_called_once_with(
            spec.Basic.Ack, 'Lb', (2, True),
        )

    def test_partial_prefix_then_rest_merges(self):
        handles = [self._deliver(tag).delivery_handle for tag in range(1, 5)]
        handles[0].ack()
        self.c.send_method.assert_called_once_with(
            spec.Basic.Ack, 'Lb', (1, False),
        )
        handles[1].ack()
        self.c.send_method.assert_called_with(
            spec.Basic.Ack, 'Lb', (2, False),
        )
        handles[3].ack()
        # tag 3 is still outstanding, so 4 cannot be acked.
        assert self.c.send_method.call_count == 2
        handles[2].ack()
        self.c.send_method.assert_called_with(
            spec.Basic.Ack, 'Lb', (4, True),
        )

    def test_reject_sends_single_frame_immediately(self):
        handles = [self._deliver(tag).delivery_handle for tag in (1, 2, 3)]
        handles[0].reject(requeue=True)
        self.c.send_method.assert_called_once_with(
            spec.Basic.Reject, 'Lb', (1, True),
        )
        handles[1].ack()
        self.c.send_method.assert_called_with(
            spec.Basic.Ack, 'Lb', (2, False),
        )
        handles[2].ack()
        self.c.send_method.assert_called_with(
            spec.Basic.Ack, 'Lb', (3, False),
        )

    def test_reject_defaults_to_no_requeue(self):
        self._deliver(1).delivery_handle.reject()
        self.c.send_method.assert_called_once_with(
            spec.Basic.Reject, 'Lb', (1, False),
        )

    def test_nack_sends_single_frame_and_defaults_to_requeue(self):
        self._deliver(1).delivery_handle.nack()
        self.c.send_method.assert_called_once_with(
            spec.Basic.Nack, 'Lbb', (1, False, True),
        )

    def test_nack_without_requeue(self):
        self._deliver(1).delivery_handle.nack(requeue=False)
        self.c.send_method.assert_called_once_with(
            spec.Basic.Nack, 'Lbb', (1, False, False),
        )

    def test_nack_does_not_coalesce(self):
        h1 = self._deliver(1).delivery_handle
        h2 = self._deliver(2).delivery_handle
        h2.nack()
        self.c.send_method.assert_called_once_with(
            spec.Basic.Nack, 'Lbb', (2, False, True),
        )
        h1.ack()
        self.c.send_method.assert_called_with(
            spec.Basic.Ack, 'Lb', (1, False),
        )

    def test_handle_can_only_be_settled_once(self):
        handle = self._deliver(1).delivery_handle
        handle.ack()
        with pytest.raises(DeliveryAlreadySettled):
            handle.ack()
        with pytest.raises(DeliveryAlreadySettled):
            handle.reject()
        with pytest.raises(DeliveryAlreadySettled):
            handle.nack()

    def test_redelivery_is_a_new_delivery_identity(self):
        first = self._deliver(1, redelivered=False).delivery_handle
        second = self._deliver(1, redelivered=True).delivery_handle

        assert first is not second
        assert first.invalid
        assert not second.invalid
        with pytest.raises(DeliveryHandleInvalid):
            first.ack()

        second.ack()
        self.c.send_method.assert_called_once_with(
            spec.Basic.Ack, 'Lb', (1, False),
        )

    def test_collect_invalidates_outstanding_handles(self):
        handle = self._deliver(1).delivery_handle
        self.c.collect()
        assert handle.invalid
        with pytest.raises(DeliveryHandleInvalid):
            handle.ack()

    def test_revive_invalidates_outstanding_handles(self):
        handle = self._deliver(1).delivery_handle
        self.c.open = Mock(name='open')
        self.c._do_revive()
        assert handle.invalid
        with pytest.raises(DeliveryHandleInvalid):
            handle.nack()

    def test_no_ack_consumer_does_not_get_a_handle(self):
        self.c.send_method.return_value = ('noack',)
        self.c.basic_consume('q', 'noack', callback=Mock(), no_ack=True)
        msg = Message()
        self.c._on_basic_deliver('noack', 1, False, 'ex', 'rkey', msg)
        assert msg.delivery_handle is None
        assert 'delivery_handle' not in msg.delivery_info

    def test_opt_in_and_no_ack_are_mutually_exclusive(self):
        with pytest.raises(ValueError):
            self.c.basic_consume(
                'q', 'bad', no_ack=True, delivery_handle=True,
            )

    def test_cancel_stops_issuing_handles(self):
        self.c.basic_cancel('ctag')
        msg = Message()
        self.c._on_basic_deliver('ctag', 1, False, 'ex', 'rkey', msg)
        assert msg.delivery_handle is None

    def test_redelivered_flag_is_recorded_on_handle(self):
        handle = self._deliver(1, redelivered=True).delivery_handle
        assert handle.redelivered is True

    def test_missing_callback_invalidates_registered_handle(self):
        self.c.delivery_handle_consumers.add('ghost')
        msg = Message()
        self.c._on_basic_deliver('ghost', 7, False, 'ex', 'rkey', msg)
        assert 7 not in self.c._deliveries
        assert msg.delivery_info['delivery_handle'].invalid


class test_DeliveryHandleBasicGet:

    @pytest.fixture(autouse=True)
    def setup_channel(self):
        self.conn = MagicMock(name='connection')
        self.conn.is_closing = False
        self.conn.channels = {}
        self.conn._get_free_channel_id.return_value = 2
        self.c = Channel(self.conn, 1)
        self.c.send_method = Mock(name='send_method')

    def _get_ok(self, tag=10):
        self.c.send_method.return_value = (
            tag, False, 'ex', 'rkey', 1, Message('body'),
        )

    def test_basic_get_opt_in_returns_handle(self):
        self._get_ok()
        msg = self.c.basic_get('q', delivery_handle=True)
        assert isinstance(msg.delivery_handle, DeliveryHandle)
        msg.delivery_handle.ack()
        self.c.send_method.assert_called_with(
            spec.Basic.Ack, 'Lb', (10, False),
        )

    def test_basic_get_without_opt_in_has_no_handle(self):
        self._get_ok()
        msg = self.c.basic_get('q')
        assert msg.delivery_handle is None
        assert 'delivery_handle' not in msg.delivery_info

    def test_basic_get_opt_in_and_no_ack_are_mutually_exclusive(self):
        with pytest.raises(ValueError):
            self.c.basic_get('q', no_ack=True, delivery_handle=True)


class test_BasicNack:

    @pytest.fixture(autouse=True)
    def setup_channel(self):
        self.conn = MagicMock(name='connection')
        self.conn.is_closing = False
        self.conn.channels = {}
        self.conn._get_free_channel_id.return_value = 2
        self.c = Channel(self.conn, 1)
        self.c.send_method = Mock(name='send_method')

    def test_basic_nack(self):
        self.c.basic_nack(123)
        self.c.send_method.assert_called_once_with(
            spec.Basic.Nack, 'Lbb', (123, False, True),
        )

    def test_basic_nack_multiple_no_requeue(self):
        self.c.basic_nack(123, multiple=True, requeue=False)
        self.c.send_method.assert_called_once_with(
            spec.Basic.Nack, 'Lbb', (123, True, False),
        )
