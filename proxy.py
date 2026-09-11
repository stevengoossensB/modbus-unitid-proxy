#!/usr/bin/env python3
"""Modbus TCP proxy that maps client Unit IDs to upstream Unit ID 0."""

import argparse
import asyncio
import logging
import os
from collections import defaultdict, deque
from typing import Optional

LOG = logging.getLogger("modbus-unitid-proxy")
MAX_MBAP_LENGTH = 253


class InvalidFrame(Exception):
    """The peer sent an invalid Modbus TCP frame."""


async def read_frame(reader: asyncio.StreamReader) -> tuple[bytes, bytes]:
    """Read one complete MBAP header and payload, preserving their bytes."""
    try:
        header = await reader.readexactly(6)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            raise
        raise InvalidFrame("truncated MBAP header") from exc

    protocol_id = int.from_bytes(header[2:4], "big")
    length = int.from_bytes(header[4:6], "big")
    # Length includes Unit ID and at least one PDU function byte.
    if protocol_id != 0 or not 2 <= length <= MAX_MBAP_LENGTH:
        raise InvalidFrame(f"invalid protocol/length: protocol={protocol_id}, length={length}")
    try:
        payload = await reader.readexactly(length)
    except asyncio.IncompleteReadError as exc:
        raise InvalidFrame("truncated Modbus payload") from exc
    return header, payload


def mapped_request(header: bytes, payload: bytes) -> tuple[int, bytes]:
    """Return the client's Unit ID and a request addressed to upstream Unit 0."""
    return payload[0], header + b"\x00" + payload[1:]


def mapped_response(header: bytes, payload: bytes, client_unit: int) -> bytes:
    """Restore the request's Unit ID while leaving every other byte untouched."""
    return header + bytes((client_unit,)) + payload[1:]


async def close_writer(writer: Optional[asyncio.StreamWriter]) -> None:
    if writer is None:
        return
    writer.close()
    try:
        await writer.wait_closed()
    except (ConnectionError, asyncio.IncompleteReadError):
        pass


class SharedUpstream:
    """Multiplex all clients over one inverter TCP connection."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.connect_lock = asyncio.Lock()
        self.write_lock = asyncio.Lock()
        self.pending: defaultdict[int, deque[tuple[asyncio.StreamWriter, int]]] = defaultdict(deque)
        self.pending_count = 0
        self.response_task: asyncio.Task[None] | None = None
        self.closed = False
        self.max_pending = 256

    async def ensure_connection(self) -> None:
        if self.writer is not None:
            return
        async with self.connect_lock:
            if self.writer is not None:
                return
            if self.closed:
                raise ConnectionError("upstream manager is closed")
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
            self.response_task = asyncio.create_task(self._forward_responses(self.reader))
            LOG.info("connected to shared upstream %s:%d", self.host, self.port)

    async def submit(
        self,
        client_writer: asyncio.StreamWriter,
        header: bytes,
        payload: bytes,
    ) -> None:
        await self.ensure_connection()
        client_unit, request = mapped_request(header, payload)
        transaction_id = int.from_bytes(header[:2], "big")
        if self.pending_count >= self.max_pending:
            raise InvalidFrame("too many pending Modbus requests")
        async with self.write_lock:
            if self.writer is None:
                raise ConnectionError("upstream disconnected")
            self.pending[transaction_id].append((client_writer, client_unit))
            self.pending_count += 1
            LOG.debug(
                "request peer=%s client_unit=%d upstream_frame=%s",
                client_writer.get_extra_info("peername"),
                client_unit,
                request.hex(),
            )
            self.writer.write(request)
            await self.writer.drain()

    async def _forward_responses(self, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                response_header, response_payload = await read_frame(reader)
                transaction_id = int.from_bytes(response_header[:2], "big")
                units = self.pending.get(transaction_id)
                if not units:
                    raise InvalidFrame(
                        f"response for unknown transaction {transaction_id}"
                    )
                client_writer, client_unit = units.popleft()
                if not units:
                    del self.pending[transaction_id]
                self.pending_count -= 1
                response = mapped_response(response_header, response_payload, client_unit)
                LOG.debug(
                    "response peer=%s client_frame=%s",
                    client_writer.get_extra_info("peername"),
                    response.hex(),
                )
                try:
                    client_writer.write(response)
                    await client_writer.drain()
                except (ConnectionError, OSError):
                    pass
        except asyncio.CancelledError:
            raise
        except (asyncio.IncompleteReadError, InvalidFrame, ConnectionError, OSError) as exc:
            LOG.warning("shared upstream connection lost: %s", exc)
        finally:
            if self.reader is reader:
                self.reader = None
                writer = self.writer
                self.writer = None
                await self._close_pending_clients()
                await close_writer(writer)

    async def _close_pending_clients(self) -> None:
        clients = {
            writer for units in self.pending.values() for writer, _ in units
        }
        self._clear_pending_clients()
        await asyncio.gather(*(close_writer(writer) for writer in clients))

    def _clear_pending_clients(self) -> None:
        self.pending.clear()
        self.pending_count = 0

    def drop_client(self, client_writer: asyncio.StreamWriter) -> None:
        for transaction_id, units in list(self.pending.items()):
            remaining = deque(
                (writer, unit) for writer, unit in units if writer is not client_writer
            )
            if remaining:
                self.pending[transaction_id] = remaining
            else:
                del self.pending[transaction_id]
        self.pending_count = sum(len(units) for units in self.pending.values())

    async def close(self) -> None:
        self.closed = True
        if self.response_task is not None:
            self.response_task.cancel()
            await asyncio.gather(self.response_task, return_exceptions=True)
            self.response_task = None
        await self._close_pending_clients()
        await close_writer(self.writer)
        self.reader = None
        self.writer = None


async def handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    *,
    upstream_host: str,
    upstream_port: int,
    upstream: SharedUpstream | None = None,
) -> None:
    peer = client_writer.get_extra_info("peername")
    owns_upstream = upstream is None
    if upstream is None:
        upstream = SharedUpstream(upstream_host, upstream_port)
    try:
        while True:
            client_header, client_payload = await read_frame(client_reader)
            await upstream.submit(client_writer, client_header, client_payload)
    except (asyncio.IncompleteReadError, InvalidFrame, ConnectionError, OSError) as exc:
        if not isinstance(exc, asyncio.IncompleteReadError) or exc.partial:
            LOG.debug("closing connection %s: %s", peer, exc)
    finally:
        upstream.drop_client(client_writer)
        await close_writer(client_writer)
        if owns_upstream:
            await upstream.close()


async def run(config: argparse.Namespace) -> None:
    shared_upstream = SharedUpstream(config.upstream_host, config.upstream_port)
    server = await asyncio.start_server(
        lambda r, w: handle_client(
            r,
            w,
            upstream_host=config.upstream_host,
            upstream_port=config.upstream_port,
            upstream=shared_upstream,
        ),
        config.listen_host,
        config.listen_port,
    )
    addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets or ())
    LOG.info(
        "listening on %s; shared upstream=%s:%d",
        addresses,
        config.upstream_host,
        config.upstream_port,
    )
    try:
        async with server:
            await server.serve_forever()
    finally:
        await shared_upstream.close()


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        number = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not 1 <= number <= 65535:
        raise ValueError(f"{name} must be between 1 and 65535")
    return number


def config_from_env() -> argparse.Namespace:
    return argparse.Namespace(
        listen_host=os.getenv("LISTEN_HOST", "0.0.0.0"),
        listen_port=env_int("LISTEN_PORT", 1502),
        upstream_host=os.getenv("UPSTREAM_HOST", "127.0.0.1"),
        upstream_port=env_int("UPSTREAM_PORT", 502),
    )


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(run(config_from_env()))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
