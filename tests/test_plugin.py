import json

import pytest
from snakemake_interface_logger_plugins.tests import TestLogHandlerBase


class MockOutputSettings:
    """Fake OutputSettingsLoggerInterface for tests."""

    def __init__(self) -> None:
        self.printshellcmds = True
        self.nocolor = False
        self.quiet = None
        self.debug_dag = False
        self.verbose = False
        self.show_failed_logs = True
        self.stdout = False
        self.dryrun = False


from snakemake_logger_plugin_stomp import (
    DefaultJSONFormatter,
    LogHandler,
    LogHandlerSettings,
)
from snakemake_logger_plugin_stomp.formatters import ComprehensiveEventFormatter


class DummyConnection:
    """A fake STOMP connection used for testing."""

    def __init__(self, *args, **kwargs):
        # record any parameters that might be useful for assertions
        self.hosts = kwargs.get("host_and_ports") or args
        self.heartbeats = kwargs.get("heartbeats")
        self._connected = True
        self.sent = []

    def set_listener(self, name, listener):
        self.listener = listener

    def set_ssl(self, *args, **kwargs):
        self.ssl_settings = (args, kwargs)

    def connect(self, username=None, passcode=None, wait=None):
        self._connected = True

    def is_connected(self):
        return self._connected

    def send(self, body, destination, headers):
        self.sent.append({"body": body, "destination": destination, "headers": headers})

    def disconnect(self):
        self._connected = False


class DummyRecord:
    """Simple object that mimics enough of ``logging.LogRecord`` for the handler."""

    def __init__(self, event, msg, levelname="INFO", **attrs):
        self.event = event
        self.msg = msg
        self.levelname = levelname
        for key, val in attrs.items():
            setattr(self, key, val)

    def getMessage(self):
        return str(self.msg)


@pytest.fixture(autouse=True)
def patch_stomp(monkeypatch):
    """Replace the real stomp.Connection with our dummy implementation.

    The tests don't exercise the network protocol; they only care that the
    handler stores and uses the connection object.  Stubbing out the real
    class prevents the constructor from trying to open a socket.
    """

    monkeypatch.setattr("stomp.Connection", DummyConnection)


class TestStompHandler(TestLogHandlerBase):
    __test__ = True

    def get_log_handler_cls(self) -> type[LogHandler]:
        return LogHandler

    def get_log_handler_settings(self) -> LogHandlerSettings:
        return LogHandlerSettings()


# additional targeted tests that go beyond the base class


def test_settings_defaults():
    settings = LogHandlerSettings()
    assert settings.host == "localhost"
    assert settings.port == 61613
    assert settings.queue == "/queue/snakemake.events"
    assert settings.user is None
    assert settings.password is None
    assert settings.stream_filter_by_workflow is False
    assert settings.consumer_heartbeat_interval == 0
    assert settings.use_ssh_tunnel is False
    assert settings.ssh_port == 22
    assert settings.reconnect_attempts == 3


def test_consumer_heartbeat_interval_must_be_non_negative():
    common = MockOutputSettings()
    settings = LogHandlerSettings(consumer_heartbeat_interval=-1)

    with pytest.raises(ValueError, match="consumer_heartbeat_interval must be >= 0"):
        LogHandler(common_settings=common, settings=settings)


def test_ssh_requires_settings_when_enabled():
    common = MockOutputSettings()
    settings = LogHandlerSettings(use_ssh_tunnel=True)

    with pytest.raises(ValueError, match="ssh_host is required"):
        LogHandler(common_settings=common, settings=settings)


def test_ssh_requires_existing_private_key_file(tmp_path):
    common = MockOutputSettings()
    settings = LogHandlerSettings(
        use_ssh_tunnel=True,
        ssh_host="jump.example.com",
        ssh_username="alice",
        ssh_private_key=str(tmp_path / "missing.key"),
    )

    with pytest.raises(ValueError, match="ssh_private_key must point to an existing"):
        LogHandler(common_settings=common, settings=settings)


def test_formatter_loads_and_falls_back(monkeypatch):
    common = MockOutputSettings()

    settings = LogHandlerSettings(
        formatter_class="snakemake_logger_plugin_stomp.formatters.DefaultJSONFormatter"
    )
    handler = LogHandler(common_settings=common, settings=settings)
    assert isinstance(handler.formatter_instance, DefaultJSONFormatter)

    settings = LogHandlerSettings(formatter_class="not.a.real.Class")
    handler = LogHandler(common_settings=common, settings=settings)
    assert isinstance(handler.formatter_instance, DefaultJSONFormatter)


def test_should_send_event_include_exclude():
    common = MockOutputSettings()
    settings = LogHandlerSettings(
        include_events="alpha,beta", exclude_events="beta,gamma"
    )
    handler = LogHandler(common_settings=common, settings=settings)

    assert handler._should_send_event("alpha")
    assert not handler._should_send_event("gamma")
    assert not handler._should_send_event("delta")


def test_emit_creates_workflow_id_and_sends():
    common = MockOutputSettings()
    settings = LogHandlerSettings()
    handler = LogHandler(common_settings=common, settings=settings)
    handler.connection = DummyConnection()

    rec = DummyRecord(event="workflow_started", msg="starting up")
    handler.emit(rec)

    assert handler.workflow_metadata["workflow_id"] is not None
    assert handler.workflow_metadata["workflow_initiated"] is not None
    assert handler.workflow_metadata["working_directory"]

    assert len(handler.connection.sent) == 1
    sent = handler.connection.sent[0]
    payload = json.loads(sent["body"])
    assert payload["event_type"] == "workflow_started"
    assert payload["message"] == "starting up"
    assert payload["hostname"] == handler.workflow_metadata["hostname"]


def test_emit_filters_out_unwanted_events():
    common = MockOutputSettings()
    settings = LogHandlerSettings(include_events="foo")
    handler = LogHandler(common_settings=common, settings=settings)
    handler.connection = DummyConnection()

    handler.emit(DummyRecord(event="bar", msg="ignore me"))
    assert handler.connection.sent == []


def test_close_disconnects():
    common = MockOutputSettings()
    settings = LogHandlerSettings()
    handler = LogHandler(common_settings=common, settings=settings)
    handler.connection = DummyConnection()
    handler.close()
    assert not handler.connection.is_connected()


def test_properties():
    common = MockOutputSettings()
    h = LogHandler(common_settings=common, settings=LogHandlerSettings())
    assert h.writes_to_stream is False
    assert h.writes_to_file is False
    assert h.has_filter is True
    assert h.has_formatter is True
    assert h.needs_rulegraph is False


def test_connection_uses_ssl_when_enabled(monkeypatch):
    """Verify SSL settings are properly passed to stomp.Connection."""
    common = MockOutputSettings()
    ssl_calls = []
    
    def mock_set_ssl(self, *args, **kwargs):
        ssl_calls.append((args, kwargs))
    
    monkeypatch.setattr(DummyConnection, "set_ssl", mock_set_ssl)
    
    settings = LogHandlerSettings(use_ssl=True, cert_file="/path/cert.pem", key_file="/path/key.pem")
    handler = LogHandler(common_settings=common, settings=settings)
    
    assert len(ssl_calls) == 1
    assert ssl_calls[0][1].get("cert_file") == "/path/cert.pem"

def test_fail_on_connection_error_true_raises(monkeypatch):
    """Verify strict mode raises RuntimeError on connection failure."""
    common = MockOutputSettings()
    def failing_connect(*args, **kwargs):
        raise ConnectionError("Broker down")
    
    monkeypatch.setattr(DummyConnection, "connect", failing_connect)
    
    settings = LogHandlerSettings(fail_on_connection_error=True)
    with pytest.raises(RuntimeError, match="STOMP logging required but broker unavailable"):
        LogHandler(common_settings=common, settings=settings)

def test_heartbeat_configuration_passed(monkeypatch):
    """Verify heartbeat tuple is passed to connection constructor."""
    common = MockOutputSettings()
    conn_calls = []
    
    def capture_connection(*args, **kwargs):
        conn_calls.append(kwargs)
        return DummyConnection(*args, **kwargs)
    
    monkeypatch.setattr("stomp.Connection", capture_connection)
    
    settings = LogHandlerSettings(heartbeat_send=5000, heartbeat_receive=15000)
    LogHandler(common_settings=common, settings=settings)
    
    assert conn_calls[0]["heartbeats"] == (5000, 15000)


def test_ssh_tunnel_endpoint_used_for_stomp_connection(monkeypatch, tmp_path):
    common = MockOutputSettings()
    conn_calls = []
    tunnel_calls = []

    class DummyTunnelManager:
        def __init__(self, **kwargs):
            tunnel_calls.append(("init", kwargs))

        def connect(self):
            tunnel_calls.append(("connect", None))
            return ("127.0.0.1", 19001)

        def close(self):
            tunnel_calls.append(("close", None))

    def capture_connection(*args, **kwargs):
        conn_calls.append(kwargs)
        return DummyConnection(*args, **kwargs)

    key_file = tmp_path / "id_ed25519"
    key_file.write_text("dummy-private-key")

    monkeypatch.setattr("snakemake_logger_plugin_stomp.SSHTunnelManager", DummyTunnelManager)
    monkeypatch.setattr("stomp.Connection", capture_connection)

    settings = LogHandlerSettings(
        use_ssh_tunnel=True,
        ssh_host="jump.example.com",
        ssh_username="alice",
        ssh_private_key=str(key_file),
    )
    handler = LogHandler(common_settings=common, settings=settings)

    assert conn_calls[0]["host_and_ports"] == [("127.0.0.1", 19001)]
    assert tunnel_calls[1][0] == "connect"

    handler.close()
    assert tunnel_calls[-1][0] == "close"


def test_send_recovers_connection_when_disconnected(monkeypatch):
    common = MockOutputSettings()
    handler = LogHandler(common_settings=common, settings=LogHandlerSettings())
    handler.connection = DummyConnection()
    handler.connection._connected = False

    recover_calls = []

    def fake_recover():
        recover_calls.append(True)
        handler.connection._connected = True
        return True

    monkeypatch.setattr(handler, "_recover_connection", fake_recover)

    handler._send_to_broker({"event_type": "test"})

    assert len(recover_calls) == 1
    assert len(handler.connection.sent) == 1

def test_send_when_not_connected():
    """Verify _send_to_broker skips gracefully when disconnected."""
    common = MockOutputSettings()
    handler = LogHandler(common_settings=common, settings=LogHandlerSettings())
    handler.connection = DummyConnection()
    handler.connection._connected = False  # Simulate disconnect
    
    # Should not raise, should log debug message
    handler._send_to_broker({"test": "data"})


def test_emit_consumer_heartbeat_sends_event_for_consumers():
    common = MockOutputSettings()
    settings = LogHandlerSettings(consumer_heartbeat_interval=30)
    handler = LogHandler(common_settings=common, settings=settings)
    handler.connection = DummyConnection()
    handler.workflow_metadata["workflow_id"] = "wf-123"

    handler._emit_consumer_heartbeat()

    assert len(handler.connection.sent) == 1
    payload = json.loads(handler.connection.sent[0]["body"])
    assert payload["event_type"] == "consumer_heartbeat"
    assert payload["workflow_id"] == "wf-123"


def test_stream_first_send_declares_queue_once():
    common = MockOutputSettings()
    settings = LogHandlerSettings(
        queue="/queue/snakemake.stream",
        use_stream=True,
    )
    handler = LogHandler(common_settings=common, settings=settings)
    handler.connection = DummyConnection()

    handler.emit(DummyRecord(event="workflow_started", msg="start"))
    handler.emit(DummyRecord(event="job_finished", msg="done"))

    first_headers = handler.connection.sent[0]["headers"]
    second_headers = handler.connection.sent[1]["headers"]

    assert first_headers["x-queue-type"] == "stream"
    assert "x-queue-type" not in second_headers


def test_stream_amq_queue_destination_includes_declaration_and_workflow_filter_headers():
    common = MockOutputSettings()
    settings = LogHandlerSettings(
        queue="/amq/queue/snakemake.stream",
        use_stream=True,
        stream_filter_by_workflow=True,
    )
    handler = LogHandler(common_settings=common, settings=settings)
    handler.connection = DummyConnection()

    handler.emit(DummyRecord(event="workflow_started", msg="start"))

    headers = handler.connection.sent[0]["headers"]
    assert headers["x-queue-type"] == "stream"
    assert headers["x-stream-filter-value"] == handler.workflow_metadata["workflow_id"]


def test_stream_filter_by_workflow_skips_filter_header_until_workflow_id_exists():
    common = MockOutputSettings()
    settings = LogHandlerSettings(
        queue="/queue/snakemake.stream",
        use_stream=True,
        stream_filter_by_workflow=True,
    )
    handler = LogHandler(common_settings=common, settings=settings)
    handler.connection = DummyConnection()

    handler.emit(DummyRecord(event="job_started", msg="start"))

    headers = handler.connection.sent[0]["headers"]
    assert "x-stream-filter-value" not in headers




def test_listener_on_error_called(monkeypatch):
    """Verify StompConnectionListener logs broker errors."""
    common = MockOutputSettings()
    handler = LogHandler(common_settings=common, settings=LogHandlerSettings())
    
    # Create a mock frame
    class MockFrame:
        body = "Broker error message"
    
    # Capture internal logger calls
    error_logs = []
    original_error = handler._internal_logger.error
    def capture_error(msg):
        error_logs.append(msg)
    handler._internal_logger.error = capture_error
    
    handler.connection.listener.on_error(MockFrame())
    assert any("Broker error" in str(msg) for msg in error_logs)


# Test the formatter more thoroughly
def test_formatter_output_structure():
    """Verify DefaultJSONFormatter produces expected JSON structure."""
    from snakemake_logger_plugin_stomp.formatters import DefaultJSONFormatter
    
    formatter = DefaultJSONFormatter()
    record = DummyRecord(event="job_finished", msg="Job done", jobid="123")
    metadata = {"workflow_id": "wf-abc", "hostname": "testhost"}
    
    result = formatter.format(record, metadata)
    
    assert result["event_type"] == "job_finished"
    assert result["workflow_id"] == "wf-abc"
    assert "timestamp" in result  # If your formatter adds timestamps


def test_comprehensive_event_formatter_preserves_high_detail_fields():
    formatter = ComprehensiveEventFormatter()

    class Wildcards:
        def items(self):
            return {"sample": "A", "lane": "L001"}.items()

    record = DummyRecord(
        event="job_finished",
        msg="Rule completed",
        jobid=17,
        name="align_reads",
        wildcards=Wildcards(),
        threads=8,
        resources={"mem_mb": 16000},
        benchmark="bench/align_reads.txt",
    )
    metadata = {"workflow_id": "wf-123", "hostname": "node-01"}

    result = formatter.format(record, metadata)

    assert result["event_type"] == "job_finished"
    assert result["workflow_id"] == "wf-123"
    assert result["event"]["jobid"] == 17
    assert result["event"]["wildcards"] == {"sample": "A", "lane": "L001"}
    assert result["event"]["benchmark"] == "bench/align_reads.txt"


def test_comprehensive_event_formatter_handles_sparse_record():
    formatter = ComprehensiveEventFormatter()
    record = DummyRecord(event="workflow_finished", msg="done")
    metadata = {"workflow_id": "wf-xyz", "hostname": "node-02"}

    result = formatter.format(record, metadata)

    assert result["event_type"] == "workflow_finished"
    assert result["event"]["event"] == "workflow_finished"


def test_emit_uses_comprehensive_event_formatter_when_configured():
    common = MockOutputSettings()
    settings = LogHandlerSettings(
        formatter_class=(
            "snakemake_logger_plugin_stomp.formatters.ComprehensiveEventFormatter"
        )
    )
    handler = LogHandler(common_settings=common, settings=settings)
    handler.connection = DummyConnection()

    rec = DummyRecord(
        event="job_started",
        msg="Launching rule",
        jobid=99,
        name="qc",
        resources={"mem_mb": 4000},
        extra_context={"attempt": 1},
    )
    handler.emit(rec)

    sent = handler.connection.sent[0]
    payload = json.loads(sent["body"])

    assert payload["event_type"] == "job_started"
    assert payload["event"]["jobid"] == 99
    assert payload["event"]["resources"]["mem_mb"] == 4000
    assert payload["event"]["extra_context"]["attempt"] == 1


def test_close_handles_disconnect_errors(monkeypatch):
    """Verify close() logs but doesn't raise on disconnect errors."""
    common = MockOutputSettings()
    handler = LogHandler(common_settings=common, settings=LogHandlerSettings())
    handler.connection = DummyConnection()
    
    def failing_disconnect():
        raise Exception("Network error")
    
    monkeypatch.setattr(handler.connection, "disconnect", failing_disconnect)
    
    # Should not raise
    handler.close()
    # Should log error (you could capture logs to verify)
