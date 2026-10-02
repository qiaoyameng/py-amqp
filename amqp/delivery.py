"""Trackable delivery handles for consumer messages."""

from .exceptions import RecoverableConnectionError

__all__ = ('DeliveryHandle', 'DeliveryTracker')


class DeliveryHandle:
    """A handle for one message delivery on a channel."""

    def __init__(self, tracker, sequence, delivery_tag):
        self._tracker = tracker
        self.sequence = sequence
        self.delivery_tag = delivery_tag
        self.settled = False
        self.invalidated = False

    @property
    def closed(self):
        return self.settled or self.invalidated or self._tracker.closed

    def ack(self):
        """Acknowledge this delivery."""
        self._tracker._settle(self, 'ack')

    def reject(self, requeue=True):
        """Reject this delivery."""
        self._tracker._settle(self, 'reject', requeue)

    def nack(self, requeue=False):
        """Reject this delivery using the RabbitMQ nack extension."""
        self._tracker._settle(self, 'nack', requeue)

    def __repr__(self):
        state = 'settled' if self.settled else 'pending'
        if self.invalidated:
            state = 'invalidated'
        return (
            f'<{type(self).__name__}: delivery_tag={self.delivery_tag!r} '
            f'state={state}>'
        )


class DeliveryTracker:
    """Track unsettled deliveries for one channel generation."""

    def __init__(self, channel, multiple_ack_allowed=True):
        self.channel = channel
        self.closed = False
        self.multiple_ack_allowed = multiple_ack_allowed
        self._next_sequence = 0
        self._next_flush_sequence = 0
        self._pending = {}
        self._completed_acks = {}
        self._completed_other = set()
        self._external_acks = set()

    def register(self, delivery_tag, redelivered=False):
        self._ensure_open()
        if redelivered:
            self._invalidate_previous_delivery(delivery_tag)
        sequence = self._next_sequence
        self._next_sequence += 1
        handle = DeliveryHandle(self, sequence, delivery_tag)
        self._pending[sequence] = (delivery_tag, handle)
        return handle

    def acknowledge_external(self, delivery_tag, multiple=False,
                             argsig='Lb'):
        self._ensure_open()
        sequences = self._select(delivery_tag, multiple)
        self.channel._basic_ack(
            delivery_tag, multiple=multiple, argsig=argsig
        )
        self._mark_settled_external(sequences, 'ack')
        self._flush_completed()

    def reject_external(self, delivery_tag, requeue, argsig='Lb'):
        self._ensure_open()
        sequences = self._select(delivery_tag, False)
        self.channel._basic_reject(
            delivery_tag, requeue, argsig=argsig
        )
        self._mark_settled_external(sequences, 'reject')
        self._flush_completed()

    def nack_external(self, delivery_tag, multiple=False, requeue=False,
                      argsig='Lbb'):
        self._ensure_open()
        sequences = self._select(delivery_tag, multiple)
        self.channel._basic_nack(
            delivery_tag, multiple=multiple, requeue=requeue,
            argsig=argsig
        )
        self._mark_settled_external(sequences, 'nack')
        self._flush_completed()

    def flush(self):
        """Send completed ACK prefixes without crossing pending messages."""
        self._ensure_open()
        self._flush_completed(force=True)

    def close(self):
        self.closed = True
        self._pending.clear()
        self._completed_acks.clear()
        self._completed_other.clear()
        self._external_acks.clear()

    def _invalidate_previous_delivery(self, delivery_tag):
        for sequence, (tag, handle) in list(self._pending.items()):
            if tag != delivery_tag:
                continue
            self._pending.pop(sequence)
            handle.invalidated = True
            self._completed_other.add(sequence)
            self.multiple_ack_allowed = False
        self._flush_completed()

    def _settle(self, handle, outcome, requeue=False):
        self._ensure_open()
        if handle.settled:
            raise RuntimeError('delivery handle already settled')
        if handle.invalidated:
            raise RecoverableConnectionError('delivery handle was redelivered')
        if handle._tracker is not self or \
                handle.sequence not in self._pending:
            raise RecoverableConnectionError(
                'delivery handle is no longer valid'
            )

        handle.settled = True
        self._pending.pop(handle.sequence)

        if outcome == 'ack':
            self._completed_acks[handle.sequence] = handle.delivery_tag
        elif outcome == 'reject':
            self.channel._basic_reject(handle.delivery_tag, requeue)
            self._completed_other.add(handle.sequence)
            self.multiple_ack_allowed = False
        else:
            self.channel._basic_nack(
                handle.delivery_tag, multiple=False, requeue=requeue
            )
            self._completed_other.add(handle.sequence)
            self.multiple_ack_allowed = False

        self._flush_completed()

    def _select(self, delivery_tag, multiple):
        return [
            sequence for sequence, (tag, handle) in self._pending.items()
            if tag == delivery_tag or (
                multiple and (delivery_tag == 0 or tag <= delivery_tag)
            )
        ]

    def _mark_settled_external(self, sequences, outcome):
        for sequence in sequences:
            try:
                delivery_tag, handle = self._pending.pop(sequence)
            except KeyError:
                continue
            handle.settled = True
            if outcome == 'ack':
                self._completed_acks[sequence] = delivery_tag
                self._external_acks.add(sequence)
            else:
                self._completed_other.add(sequence)
        if outcome != 'ack':
            self.multiple_ack_allowed = False

    def _flush_completed(self, force=False):
        flush_sequence = self._next_flush_sequence
        sequence = flush_sequence
        while sequence in self._completed_acks or \
                sequence in self._completed_other or \
                sequence in self._external_acks:
            if sequence in self._completed_other:
                self._completed_other.remove(sequence)
                sequence += 1
                flush_sequence = sequence
                continue
            if sequence in self._external_acks:
                self._external_acks.remove(sequence)
                self._completed_acks.pop(sequence, None)
                sequence += 1
                flush_sequence = sequence
                continue

            run = []
            while sequence in self._completed_acks:
                run.append((sequence, self._completed_acks.pop(sequence)))
                sequence += 1

            next_sequence_is_pending = sequence in self._pending
            if self.multiple_ack_allowed and len(run) > 1:
                self.channel._basic_ack(run[-1][1], multiple=True)
                flush_sequence = sequence
            elif self.multiple_ack_allowed and \
                    next_sequence_is_pending and not force:
                for completed_sequence, delivery_tag in reversed(run):
                    self._completed_acks[completed_sequence] = delivery_tag
                sequence = flush_sequence
                break
            else:
                for _, delivery_tag in run:
                    self.channel._basic_ack(delivery_tag, multiple=False)
                self.multiple_ack_allowed = False
                flush_sequence = sequence

        self._next_flush_sequence = flush_sequence

    def _ensure_open(self):
        if self.closed:
            raise RecoverableConnectionError('channel already closed')
