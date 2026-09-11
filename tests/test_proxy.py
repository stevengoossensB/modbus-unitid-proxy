import asyncio
import importlib.util
import pathlib
import unittest


ROOT = pathlib.Path(__file__).parent.parent
spec = importlib.util.spec_from_file_location("proxy", ROOT / "proxy.py")
proxy = importlib.util.module_from_spec(spec)
# Tests intentionally import the implementation from the project root.
spec.loader.exec_module(proxy)


class FakeUpstream:
    def __init__(self, handler):
        self.handler = handler
        self.server = None
        self.connections = 0

    async def start(self):
        self.server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    async def _accept(self, reader, writer):
        self.connections += 1
        try:
            await self.handler(reader, writer)
        finally:
            writer.close()
            await writer.wait_closed()

    async def close(self):
        self.server.close()
        await self.server.wait_closed()


class ProxyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.upstream_writers = []

    async def asyncTearDown(self):
        for writer in self.upstream_writers:
            writer.close()
            await writer.wait_closed()
        if hasattr(self, "proxy_server"):
            self.proxy_server.close()
            await self.proxy_server.wait_closed()
        if hasattr(self, "upstream"):
            await self.upstream.close()

    async def start_proxy(self, upstream_port):
        self.proxy_server = await asyncio.start_server(
            lambda r, w: proxy.handle_client(
                r, w, upstream_host="127.0.0.1", upstream_port=upstream_port
            ),
            "127.0.0.1",
            0,
        )
        return self.proxy_server.sockets[0].getsockname()[1]

    @staticmethod
    def frame(transaction, unit, pdu):
        return transaction.to_bytes(2, "big") + b"\x00\x00" + (len(pdu) + 1).to_bytes(2, "big") + bytes([unit]) + pdu

    async def read_frame(self, reader):
        header = await reader.readexactly(6)
        length = int.from_bytes(header[4:6], "big")
        return header + await reader.readexactly(length)

    async def test_fragmented_request_is_rewritten_and_response_restored(self):
        seen = []

        async def handler(reader, writer):
            request = await self.read_frame(reader)
            seen.append(request)
            response = request[:6] + b"\x00" + request[7:]
            writer.write(response[:3])
            await writer.drain()
            await asyncio.sleep(0)
            writer.write(response[3:])
            await writer.drain()

        self.upstream = FakeUpstream(handler)
        upstream_port = await self.upstream.start()
        port = await self.start_proxy(upstream_port)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        request = self.frame(0x1234, 7, b"\x03\x00\x10\x00\x02")
        for byte in request:
            writer.write(bytes([byte]))
            await writer.drain()
        response = await self.read_frame(reader)
        self.assertEqual(seen, [request[:6] + b"\x00" + request[7:]])
        self.assertEqual(response, request)
        writer.close()
        await writer.wait_closed()

    async def test_multiple_requests_and_concurrent_clients(self):
        async def handler(reader, writer):
            while True:
                try:
                    request = await self.read_frame(reader)
                except asyncio.IncompleteReadError:
                    return
                response = request[:6] + b"\x00" + request[7:]
                writer.write(response)
                await writer.drain()

        self.upstream = FakeUpstream(handler)
        upstream_port = await self.upstream.start()
        port = await self.start_proxy(upstream_port)

        async def client(transaction, unit):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            requests = [self.frame(transaction, unit, b"\x06\x00\x01\x00\x02"), self.frame(transaction + 1, unit, b"\x03\x00\x00\x00\x01")]
            writer.write(b"".join(requests))
            await writer.drain()
            result = [await self.read_frame(reader), await self.read_frame(reader)]
            writer.close()
            await writer.wait_closed()
            return result

        results = await asyncio.gather(client(1, 1), client(2, 253))
        self.assertEqual(results[0], [self.frame(1, 1, b"\x06\x00\x01\x00\x02"), self.frame(2, 1, b"\x03\x00\x00\x00\x01")])
        self.assertEqual(results[1], [self.frame(2, 253, b"\x06\x00\x01\x00\x02"), self.frame(3, 253, b"\x03\x00\x00\x00\x01")])

    async def test_retry_while_first_response_pending(self):
        async def handler(reader, writer):
            first = await self.read_frame(reader)
            second = await self.read_frame(reader)
            self.assertEqual(first[6], 0)
            self.assertEqual(second[6], 0)
            response = second[:6] + b"\x00" + second[7:]
            writer.write(response)
            await writer.drain()

        self.upstream = FakeUpstream(handler)
        upstream_port = await self.upstream.start()
        port = await self.start_proxy(upstream_port)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        first = self.frame(1, 1, b"\x03\x75\x30\x00\x0f")
        second = self.frame(2, 1, b"\x03\x75\x30\x00\x0f")
        writer.write(first)
        await writer.drain()
        await asyncio.sleep(0.01)
        writer.write(second)
        await writer.drain()
        response = await self.read_frame(reader)
        self.assertEqual(response, second)
        writer.close()
        await writer.wait_closed()

    async def test_malformed_frame_closes_connection(self):
        async def handler(reader, writer):
            await asyncio.sleep(1)

        self.upstream = FakeUpstream(handler)
        upstream_port = await self.upstream.start()
        port = await self.start_proxy(upstream_port)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"\x00\x01\x00\x01\x00\x01")  # invalid protocol and length
        await writer.drain()
        self.assertEqual(await reader.read(), b"")
        writer.close()
        await writer.wait_closed()

    async def test_upstream_disconnect_closes_client(self):
        async def handler(reader, writer):
            await reader.read(7)
            writer.close()
            await writer.wait_closed()

        self.upstream = FakeUpstream(handler)
        upstream_port = await self.upstream.start()
        port = await self.start_proxy(upstream_port)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(self.frame(1, 9, b"\x01\x00"))
        await writer.drain()
        self.assertEqual(await reader.read(), b"")
        writer.close()
        await writer.wait_closed()


if __name__ == "__main__":
    unittest.main()
