"""Structures used by graceful connection shutdown."""

from dataclasses import dataclass


@dataclass(frozen=True)
class PendingPublish:
    """Publisher confirm that was not resolved before shutdown."""

    channel_id: int
    delivery_tag: int
    exchange: str
    routing_key: str


@dataclass(frozen=True)
class PendingConsumerCancellation:
    """Consumer cancellation that did not receive cancel-ok."""

    channel_id: int
    consumer_tag: str


@dataclass
class GracefulCloseResult:
    """Terminal state shared by every graceful-close invocation."""

    timed_out: bool = False
    unconfirmed_publishes: tuple = ()
    uncancelled_consumers: tuple = ()
    errors: tuple = ()

    @property
    def complete(self):
        return not (
            self.timed_out or
            self.unconfirmed_publishes or
            self.uncancelled_consumers or
            self.errors
        )
