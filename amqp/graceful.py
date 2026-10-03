from collections import namedtuple

PendingPublish = namedtuple('PendingPublish', (
    'channel_id', 'delivery_tag', 'exchange', 'routing_key',
))
PendingConsumerCancel = namedtuple('PendingConsumerCancel', (
    'channel_id', 'consumer_tag',
))
GracefulCloseResult = namedtuple('GracefulCloseResult', (
    'timed_out', 'pending_publishes', 'pending_consumer_cancels',
))
