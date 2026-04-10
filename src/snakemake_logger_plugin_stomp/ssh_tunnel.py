"""SSH tunnel utilities for STOMP broker connectivity."""

import logging
import select
import socketserver
import threading
from typing import Optional

import paramiko


class _ForwardHandler(socketserver.BaseRequestHandler):
    """Forward one local TCP connection to the remote broker through SSH."""

    ssh_transport: paramiko.Transport
    remote_host: str
    remote_port: int

    def handle(self) -> None:
        try:
            channel = self.ssh_transport.open_channel(
                "direct-tcpip",
                (self.remote_host, self.remote_port),
                self.request.getpeername(),
            )
        except Exception:
            return

        if channel is None:
            return

        try:
            while True:
                readable, _, _ = select.select([self.request, channel], [], [])
                if self.request in readable:
                    data = self.request.recv(4096)
                    if not data:
                        break
                    channel.sendall(data)

                if channel in readable:
                    data = channel.recv(4096)
                    if not data:
                        break
                    self.request.sendall(data)
        finally:
            channel.close()
            self.request.close()


class _ForwardServer(socketserver.ThreadingTCPServer):
    """Threaded local server that forwards connections over SSH."""

    daemon_threads = True
    allow_reuse_address = True


class SSHTunnelManager:
    """Manage SSH tunnel lifecycle for STOMP connectivity."""

    def __init__(
        self,
        ssh_host: str,
        ssh_port: int,
        ssh_username: str,
        ssh_private_key: str,
        ssh_key_passphrase: Optional[str],
        remote_host: str,
        remote_port: int,
        local_bind_port: int,
        connect_timeout: int,
        logger: logging.Logger,
    ) -> None:
        self._ssh_host = ssh_host
        self._ssh_port = ssh_port
        self._ssh_username = ssh_username
        self._ssh_private_key = ssh_private_key
        self._ssh_key_passphrase = ssh_key_passphrase
        self._remote_host = remote_host
        self._remote_port = remote_port
        self._local_bind_port = local_bind_port
        self._connect_timeout = connect_timeout
        self._logger = logger

        self._ssh_client: Optional[paramiko.SSHClient] = None
        self._forward_server: Optional[_ForwardServer] = None
        self._forward_thread: Optional[threading.Thread] = None
        self._endpoint: Optional[tuple[str, int]] = None

    def connect(self) -> tuple[str, int]:
        """Create a tunnel and return local endpoint for STOMP connection."""
        if self._endpoint is not None:
            return self._endpoint

        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.set_missing_host_key_policy(paramiko.RejectPolicy())
        ssh_client.connect(
            hostname=self._ssh_host,
            port=self._ssh_port,
            username=self._ssh_username,
            key_filename=self._ssh_private_key,
            passphrase=self._ssh_key_passphrase,
            timeout=self._connect_timeout,
            allow_agent=False,
            look_for_keys=False,
        )

        transport = ssh_client.get_transport()
        if transport is None:
            ssh_client.close()
            raise RuntimeError("SSH transport was not established")

        transport.set_keepalive(30)

        class Handler(_ForwardHandler):
            ssh_transport = transport
            remote_host = self._remote_host
            remote_port = self._remote_port

        forward_server = _ForwardServer(("127.0.0.1", self._local_bind_port), Handler)
        forward_thread = threading.Thread(
            target=forward_server.serve_forever,
            name="stomp-ssh-forwarder",
            daemon=True,
        )
        forward_thread.start()

        self._ssh_client = ssh_client
        self._forward_server = forward_server
        self._forward_thread = forward_thread
        self._endpoint = forward_server.server_address

        self._logger.info(
            "[STOMP] Opened SSH tunnel "
            f"127.0.0.1:{self._endpoint[1]} -> {self._remote_host}:{self._remote_port} "
            f"via {self._ssh_host}:{self._ssh_port}"
        )
        return self._endpoint

    def close(self) -> None:
        """Close local forward server and SSH session."""
        if self._forward_server is not None:
            self._forward_server.shutdown()
            self._forward_server.server_close()

        if self._forward_thread is not None and self._forward_thread.is_alive():
            self._forward_thread.join(timeout=2)

        if self._ssh_client is not None:
            self._ssh_client.close()

        self._forward_server = None
        self._forward_thread = None
        self._ssh_client = None
        self._endpoint = None
