"""Opt-in delivery handles for incoming messages.

When a consumer opts in via ``basic_consume(..., delivery_handle=True)``,
each delivered :class:`~amqp.basic_message.Message` carries a
:class:`DeliveryHandle` that is bound to that specific delivery on the
channel.  The channel tracks every outstanding handle, which removes the
need to juggle ``delivery_tag`` values and ``multiple=True`` by hand.

A handle can settle a message exactly once, using :meth:`DeliveryHandle.ack`,
:meth:`DeliveryHandle.reject` or :meth:`DeliveryHandle.nack`.  Handles
acknowledging a completed prefix are coalesced into a single
``basic.ack`` frame, never skipping past a message that is still
outstanding.
"""


__all__ = (
    'DeliveryError',
    'DeliveryAlreadySettled',
    'DeliveryHandleInvalid',
    'DeliveryHandle',
)


class DeliveryError(Exception):
    """Base class for delivery handle errors."""


class DeliveryAlreadySettled(DeliveryError):
    """A settlement method was called more than once on a handle."""


class DeliveryHandleInvalid(DeliveryError):
    """The handle no longer refers to a live delivery on its channel.

    This is raised for messages delivered to a ``no_ack`` consumer, or
    when the originating channel has been closed (or revived) before
    the handle was settled.
    """


class DeliveryHandle:
    """A tracked, one-shot settlement handle for a delivered message.

    Instances are created by the channel when a consumer (or
    ``basic_get`` call) opts in with ``delivery_handle=True``.  They
    should not be instantiated directly.

    A delivery tag is only meaningful for a single delivery on a single
    channel: redelivered messages receive a fresh handle even when they
    carry the same tag after a channel recovery.
    """

    def __init__(self, channel, delivery_tag, *, consumer_tag=None):
        self._channel = channel
        self.delivery_tag = delivery_tag
        self.consumer_tag = consumer_tag
        self.redelivered = False
        self.settled = False
        self.settlement = None
        self._invalid = False

    @property
    def invalid(self):
        """True when the handle can no longer settle its delivery."""
        return self._invalid or self._channel is None

    def _require_usable(self):
        if self.settled:
            raise DeliveryAlreadySettled(
                f'Delivery {self.delivery_tag!r} is already settled '
                f'({self.settlement})'
            )
        if self._invalid or self._channel is None:
            raise DeliveryHandleInvalid(
                f'Delivery handle for tag {self.delivery_tag!r} is no '
                'longer valid: its channel was closed or the message '
                'was redelivered'
            )

    def ack(self):
        """Acknowledge the message.

        The frame may be deferred and merged with other completed
        handles that form a contiguous prefix of outstanding messages.
        """
        self._require_usable()
        self.settled = True
        self.settlement = 'ack'
        self._channel._settle_delivery(self, 'ack')

    def reject(self, requeue=False):
        """Reject the message, optionally requeuing it.

        Sends a single ``basic.reject`` frame immediately.
        """
        self._require_usable()
        self.settled = True
        self.settlement = 'reject'
        self._channel._settle_delivery(
            self, 'reject', requeue=requeue,
        )

    def nack(self, requeue=True):
        """Negatively acknowledge the message, optionally requeuing it.

        Sends a single ``basic.nack`` frame immediately, with
        ``multiple`` always False: coalescing across messages is only
        performed for :meth:`ack`.
        """
        self._require_usable()
        self.settled = True
        self.settlement = 'nack'
        self._channel._settle_delivery(
            self, 'nack', requeue=requeue,
        )

    def _invalidate(self):
        self._invalid = True
        self._channel = None
