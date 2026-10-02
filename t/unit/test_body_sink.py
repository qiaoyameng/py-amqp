import io

import pytest

from amqp.body_sink import (DEFAULT_BODY_SPOOL_SIZE, BytesBodySink, FileBody,
                            SpooledBodySink, spooled_body_sink)


class test_FileBody:

    @pytest.fixture(autouse=True)
    def setup_body(self):
        self.fileobj = io.BytesIO(b'hello world')
        self.body = FileBody(self.fileobj)

    def test_read(self):
        assert self.body.read(5) == b'hello'
        assert self.body.read() == b' world'

    def test_seek_tell(self):
        self.body.seek(6)
        assert self.body.tell() == 6
        assert self.body.read() == b'world'

    def test_close(self):
        assert not self.body.closed
        self.body.close()
        assert self.body.closed
        assert self.fileobj.closed
        # closing twice is fine
        self.body.close()

    def test_context_manager(self):
        with self.body as body:
            assert body.read(5) == b'hello'
        assert self.body.closed

    def test_does_not_expose_file_path(self):
        assert not hasattr(self.body, 'name')
        assert not hasattr(self.body, 'path')


class test_BytesBodySink:

    def test_finalize_returns_bytes(self):
        sink = BytesBodySink()
        sink.write(b'thequick')
        sink.write(b'brownfox')
        assert sink.finalize() == b'thequickbrownfox'

    def test_close_discards_content(self):
        sink = BytesBodySink()
        sink.write(b'thequick')
        sink.close()
        sink.close()  # idempotent


class test_SpooledBodySink:

    def test_small_body_stays_in_memory(self):
        sink = SpooledBodySink(max_size=16)
        sink.write(b'thequick')
        sink.write(b'brownfox')
        body = sink.finalize()
        assert type(body) is bytes
        assert body == b'thequickbrownfox'

    def test_large_body_rolls_over_to_file(self):
        sink = SpooledBodySink(max_size=8)
        sink.write(b'thequick')
        assert not sink._rolled
        sink.write(b'brownfox')
        assert sink._rolled
        body = sink.finalize()
        assert isinstance(body, FileBody)
        try:
            assert body.read() == b'thequickbrownfox'
        finally:
            body.close()
        assert body.closed

    def test_body_at_threshold_stays_in_memory(self):
        sink = SpooledBodySink(max_size=16)
        sink.write(b'thequickbrownfox')
        body = sink.finalize()
        assert type(body) is bytes

    def test_close_releases_temp_file(self):
        sink = SpooledBodySink(max_size=1)
        sink.write(b'thequickbrownfox')
        fileobj = sink._file
        sink.close()
        assert fileobj.closed
        assert sink._file is None
        sink.close()  # idempotent

    def test_close_in_memory(self):
        sink = SpooledBodySink(max_size=128)
        sink.write(b'thequick')
        sink.close()
        assert sink._file is None


class test_spooled_body_sink:

    def test_factory_defaults(self):
        factory = spooled_body_sink()
        sink = factory(10, channel=1)
        assert isinstance(sink, SpooledBodySink)
        assert sink.max_size == DEFAULT_BODY_SPOOL_SIZE

    def test_factory_custom_max_size(self):
        factory = spooled_body_sink(max_size=42)
        sink = factory(100, channel=2)
        assert sink.max_size == 42
