import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import asyncssh
from asyncssh import SSHCompletedProcess

from viva_api.common.storage.file_paths import HPCFilePath

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SSHSession:
    """A self-healing SSH session that reconnects on connection failures.

    This wrapper around an SSH connection provides command execution and file transfer
    with automatic reconnection when the connection is lost.
    """

    def __init__(
        self,
        conn: asyncssh.SSHClientConnection,
        hostname: str,
        connect_fn: Callable[[], Awaitable[asyncssh.SSHClientConnection]] | None = None,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ) -> None:
        self._conn = conn
        self._hostname = hostname
        self._connect_fn = connect_fn
        self._max_retries = max_retries
        self._retry_delay = retry_delay

    @property
    def connection(self) -> asyncssh.SSHClientConnection:
        """Access the underlying asyncssh connection."""
        return self._conn

    def close(self) -> None:
        """Close the underlying connection."""
        if self._conn is not None:
            self._conn.close()

    async def wait_closed(self) -> None:
        """Wait for the connection to fully close."""
        if self._conn is not None:
            await self._conn.wait_closed()

    async def _reconnect(self) -> None:
        """Reconnect to the remote host."""
        if self._connect_fn is None:
            raise RuntimeError("Cannot reconnect: no connection factory provided")
        logger.info(f"Reconnecting to {self._hostname}...")
        if self._conn is not None:
            with contextlib.suppress(Exception):
                self._conn.close()
        self._conn = await self._connect_fn()
        logger.info(f"Reconnected to {self._hostname}")

    def _is_connection_error(self, exc: Exception) -> bool:
        """Check if exception indicates a lost connection."""
        if isinstance(exc, asyncssh.misc.ChannelOpenError | asyncssh.misc.ConnectionLost | OSError):
            return True
        exc_str = str(exc).lower()
        return "connection closed" in exc_str or "connection lost" in exc_str

    async def run_command(self, command: str, check: bool = True) -> tuple[int, str, str]:
        """Run a command on the remote host with automatic reconnection.

        :param command: The command to execute
        :param check: If True (default), raise RuntimeError on non-zero exit code.
                      If False, return the tuple and let the caller handle non-zero exit codes.
        :return: Tuple of (return_code, stdout, stderr)
        :raises RuntimeError: If check=True and command returns non-zero exit code,
                              or if connection fails after all retries
        """
        last_exc: Exception | None = None

        for attempt in range(self._max_retries + 1):
            try:
                logger.info(f"Running SSH command: {command}")
                result: SSHCompletedProcess = await self._conn.run(command, check=False)

                if not isinstance(result.stdout, str):
                    raise TypeError(f"Expected result.stdout to be str, got {type(result.stdout)}")
                if not isinstance(result.stderr, str):
                    raise TypeError(f"Expected result.stderr to be str, got {type(result.stderr)}")
                if not isinstance(result.returncode, int):
                    raise TypeError(f"Expected result.returncode to be int, got {type(result.returncode)}")

                log_level = logging.INFO if result.returncode == 0 else logging.WARNING
                logger.log(
                    log_level,
                    f"Command '{command}' retcode={result.returncode} "
                    f"stdout={result.stdout[:100]} stderr={result.stderr[:100]!r}",
                )

                # Raise exception on non-zero exit if check=True (default, preserves original behavior)
                if check and result.returncode != 0:
                    raise RuntimeError(
                        f"SSH command failed with exit code {result.returncode}: {result.stderr[:100]!r}"
                    )

                return result.returncode, result.stdout, result.stderr

            except (OSError, asyncssh.Error) as exc:
                last_exc = exc
                can_retry = (
                    self._connect_fn is not None and attempt < self._max_retries and self._is_connection_error(exc)
                )
                if can_retry:
                    logger.warning(
                        f"Connection error on attempt {attempt + 1}/{self._max_retries + 1}, "
                        f"reconnecting in {self._retry_delay}s: {exc}"
                    )
                    await asyncio.sleep(self._retry_delay)
                    try:
                        await self._reconnect()
                    except Exception as reconnect_exc:
                        logger.warning(f"Reconnection failed: {reconnect_exc}")
                        last_exc = reconnect_exc
                else:
                    logger.exception(f"SSH error while running '{command}'")
                    raise RuntimeError(f"SSH command error: {str(exc)[:100]}") from exc

        raise RuntimeError(f"SSH command failed after {self._max_retries} retries") from last_exc

    async def scp_upload(self, local_file: Path, remote_path: HPCFilePath, **kwargs: Any) -> None:
        """Upload a file to the remote host via SCP with automatic reconnection.

        :param local_file: Path to the local file
        :param remote_path: Remote destination path
        :raises RuntimeError: If the upload fails after all retries
        """
        last_exc: Exception | None = None

        for attempt in range(self._max_retries + 1):
            try:
                await asyncssh.scp(local_file, (self._conn, remote_path.remote_path), **kwargs)
                logger.info(f"Uploaded {local_file} -> {remote_path}")
                return

            except (OSError, asyncssh.Error) as exc:
                last_exc = exc
                can_retry = (
                    self._connect_fn is not None and attempt < self._max_retries and self._is_connection_error(exc)
                )
                if can_retry:
                    logger.warning(
                        f"Connection error on upload attempt {attempt + 1}/{self._max_retries + 1}, "
                        f"reconnecting in {self._retry_delay}s: {exc}"
                    )
                    await asyncio.sleep(self._retry_delay)
                    try:
                        await self._reconnect()
                    except Exception as reconnect_exc:
                        logger.warning(f"Reconnection failed: {reconnect_exc}")
                        last_exc = reconnect_exc
                else:
                    logger.exception(f"Failed to upload {local_file}")
                    raise RuntimeError(f"SCP upload failed: {str(exc)[:100]}") from exc

        raise RuntimeError(f"SCP upload failed after {self._max_retries} retries") from last_exc

    async def scp_download(self, local_file: Path, remote_path: HPCFilePath) -> None:
        """Download a file from the remote host via SCP with automatic reconnection.

        :param local_file: Local destination path
        :param remote_path: Path to the remote file
        :raises RuntimeError: If the download fails after all retries
        """
        last_exc: Exception | None = None

        for attempt in range(self._max_retries + 1):
            try:
                await asyncssh.scp((self._conn, remote_path.remote_path), local_file)
                logger.info(f"Downloaded {remote_path} -> {local_file}")
                return

            except (OSError, asyncssh.Error) as exc:
                last_exc = exc
                can_retry = (
                    self._connect_fn is not None and attempt < self._max_retries and self._is_connection_error(exc)
                )
                if can_retry:
                    logger.warning(
                        f"Connection error on download attempt {attempt + 1}/{self._max_retries + 1}, "
                        f"reconnecting in {self._retry_delay}s: {exc}"
                    )
                    await asyncio.sleep(self._retry_delay)
                    try:
                        await self._reconnect()
                    except Exception as reconnect_exc:
                        logger.warning(f"Reconnection failed: {reconnect_exc}")
                        last_exc = reconnect_exc
                else:
                    logger.exception(f"Failed to download {remote_path}")
                    raise RuntimeError(f"SCP download failed: {str(exc)[:100]}") from exc

        raise RuntimeError(f"SCP download failed after {self._max_retries} retries") from last_exc


class SSHSessionService:
    """SSH service that provides sessions via an async context manager.

    Each session creates a fresh SSH connection, verifies it with a ping,
    and cleanly closes it when the context exits. Sessions support automatic
    reconnection if the connection is lost during operations.

    Example:
        service = SSHSessionService(hostname="example.com", username="user", key_path=Path("~/.ssh/id_rsa"))
        async with service.session() as ssh:
            retcode, stdout, stderr = await ssh.run_command("ls -la")
    """

    def __init__(
        self,
        hostname: str,
        username: str,
        key_path: Path,
        known_hosts: Path | None = None,
        keepalive_interval: int = 30,
    ) -> None:
        self.hostname = hostname
        self.username = username
        self.key_path = key_path
        self.known_hosts = str(known_hosts) if known_hosts else None
        self.keepalive_interval = keepalive_interval

    async def _create_connection(self) -> asyncssh.SSHClientConnection:
        """Create a new SSH connection."""
        return await asyncssh.connect(
            host=self.hostname,
            username=self.username,
            client_keys=[self.key_path],
            known_hosts=self.known_hosts,
            keepalive_interval=self.keepalive_interval,
        )

    @asynccontextmanager
    async def session(self, wait_closed: bool = True) -> AsyncIterator[SSHSession]:
        """Create and yield an SSH session.

        Creates a new SSH connection, verifies it with a ping command,
        then yields an SSHSession for command execution. On exit, the
        connection is closed. The session supports automatic reconnection
        if the connection is lost during operations.

        :param wait_closed: If True (default), wait for the connection to fully close
            before returning from the context manager. Set to False for faster exit
            when you don't need to guarantee the connection is fully terminated.
        :yields: An SSHSession instance for running commands and transferring files
        :raises RuntimeError: If connection or verification fails
        """
        conn: asyncssh.SSHClientConnection | None = None
        ssh_session: SSHSession | None = None
        try:
            logger.info(f"Opening SSH session to {self.hostname} as {self.username}")
            conn = await self._create_connection()

            # Verify connection with a ping
            result = await conn.run("echo ping", check=True)
            if result.stdout is None or "ping" not in result.stdout:
                raise RuntimeError("SSH connection verification failed: ping did not return expected output")

            logger.info(f"SSH session established and verified to {self.hostname}")
            ssh_session = SSHSession(
                conn=conn,
                hostname=self.hostname,
                connect_fn=self._create_connection,
            )
            yield ssh_session

        except (OSError, asyncssh.Error) as exc:
            logger.exception(f"Failed to establish SSH session to {self.hostname}")
            raise RuntimeError(f"SSH session failed: {str(exc)[:100]}") from exc

        finally:
            if ssh_session is not None:
                logger.info(f"Closing SSH session to {self.hostname}")
                ssh_session.close()
                if wait_closed:
                    try:
                        await ssh_session.wait_closed()
                        logger.info(f"SSH session to {self.hostname} fully closed")
                    except Exception as exc:
                        logger.warning(f"Error while waiting for SSH connection to close: {exc}")
                else:
                    logger.info(f"SSH session to {self.hostname} close initiated (not waiting)")
            elif conn is not None:
                # Connection was created but session setup failed (e.g., ping verification failed)
                logger.info(f"Closing SSH connection to {self.hostname} (session setup failed)")
                conn.close()
                if wait_closed:
                    try:
                        await conn.wait_closed()
                    except Exception as exc:
                        logger.warning(f"Error while waiting for SSH connection to close: {exc}")
