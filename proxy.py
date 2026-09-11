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


async def handle_client(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
    *,
    upstream_host: str,
    upstream_port: int,
) -> None:
    peer = client_writer.get_extra_info("peername")
    upstream_writer = None
    pending_units: defaultdict[int, deque[int]] = defaultdict(deque)
    pending_count = 0
    max_pending = 128

    async def forward_requests(
        upstream_writer: asyncio.StreamWriter,
    ) -> None:
        nonlocal pending_count
        while True:
            client_header, client_payload = await read_frame(client_reader)
            if pending_count >= max_pending:
                raise InvalidFrame("too many pending Modbus requests")
            client_unit, request = mapped_request(client_header, client_payload)
            transaction_id = int.from_bytes(client_header[:2], "big")
            pending_units[transaction_id].append(client_unit)
            pending_count += 1
            LOG.debug(
                "request peer=%s client_unit=%d upstream_frame=%s",
                peer,
                client_unit,
                request.hex(),
            )
            upstream_writer.write(request)
            await upstream_writer.drain()

    async def forward_responses(
        upstream_reader: asyncio.StreamReader,
    ) -> None:
        nonlocal pending_count
        while True:
            response_header, response_payload = await read_frame(upstream_reader)
            transaction_id = int.from_bytes(response_header[:2], "big")
            units = pending_units.get(transaction_id)
            if not units:
                raise InvalidFrame(
                    f"response for unknown transaction {transaction_id}"
                )
            client_unit = units.popleft()
            if not units:
                del pending_units[transaction_id]
            pending_count -= 1
            response = mapped_response(response_header, response_payload, client_unit)
            LOG.debug("response peer=%s client_frame=%s", peer, response.hex())
            client_writer.write(response)
            await client_writer.drain()

    try:
        upstream_reader, upstream_writer = await asyncio.open_connection(
            upstream_host, upstream_port
        )
        request_task = asyncio.create_task(forward_requests(upstream_writer))
        response_task = asyncio.create_task(forward_responses(upstream_reader))
        done, pending = await asyncio.wait(
            (request_task, response_task),
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            task.result()
    except (asyncio.IncompleteReadError, InvalidFrame, ConnectionError, OSError) as exc:
        if not isinstance(exc, asyncio.IncompleteReadError) or exc.partial:
            LOG.debug("closing connection %s: %s", peer, exc)
    finally:
        await close_writer(upstream_writer)
        await close_writer(client_writer)


async def run(config: argparse.Namespace) -> None:
    server = await asyncio.start_server(
        lambda r, w: handle_client(
            r,
            w,
            upstream_host=config.upstream_host,
            upstream_port=config.upstream_port,
        ),
        config.listen_host,
        config.listen_port,
    )
    addresses = ", ".join(str(sock.getsockname()) for sock in server.sockets or ())
    LOG.info("listening on %s; upstream=%s:%d", addresses, config.upstream_host, config.upstream_port)
    async with server:
        await server.serve_forever()


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
