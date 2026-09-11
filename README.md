# Modbus TCP Unit-ID proxy

A small asyncio proxy for devices that send a nonzero Modbus TCP Unit ID to a
server that requires Unit ID `0` (for example, an inverter behind a Jullix).

For each client connection, the proxy opens one upstream connection and relays
requests in order. On requests it changes only the MBAP Unit ID byte to `0`.
On responses it changes only the Unit ID byte back to the value from the
corresponding request. Transaction ID, protocol ID, MBAP length, and PDU bytes
are otherwise copied unchanged.

## Configuration

Configuration is through environment variables:

| Variable | Default | Meaning |
|---|---:|---|
| `LISTEN_HOST` | `0.0.0.0` | Local bind address |
| `LISTEN_PORT` | `1502` | Local TCP port (use a high port when running unprivileged) |
| `UPSTREAM_HOST` | `127.0.0.1` | Inverter/server address |
| `UPSTREAM_PORT` | `502` | Inverter/server TCP port |
| `LOG_LEVEL` | `INFO` | Python logging level |

Port values must be integers from 1 through 65535. The default container port
is 1502 because port 502 requires elevated bind privileges on Linux. The
provided Compose file maps host port 502—the port Jullix should use—to container
port 1502.

## Docker Compose

Copy `docker-compose.yml.example` to `docker-compose.yml`, set
`UPSTREAM_HOST`, and start it:

```sh
cp docker-compose.yml.example docker-compose.yml
docker compose up -d --build
```

The container uses only Python's standard library; there are no pip
requirements. The image runs as the unprivileged `nobody` user.

## Protocol and failure behavior

* TCP fragmentation and multiple complete requests per connection are handled.
* Each client gets an independent upstream connection, so clients can operate
  concurrently without sharing request/response Unit-ID state.
* Invalid MBAP protocol IDs or lengths, truncated frames, and upstream
  disconnects close the affected client connection. No malformed data is
  forwarded.
* MBAP length must be 2..253 (Unit ID plus at least one PDU byte), as required
  for a bounded Modbus TCP ADU.
* The proxy does not inspect or validate function codes, exception responses,
  transaction matching, or application semantics. It assumes the upstream
  returns one response for each request, in order, and preserves the MBAP
  header fields other than Unit ID.
* There is no authentication, encryption, allow-list, or request timeout. Keep
  it on a trusted network and use network/firewall controls as appropriate.
* This is a Unit-ID mapper, not a Modbus gateway: it does not translate serial
  addressing or multiplex several upstream devices.

## Tests

The test suite uses only `unittest` and local asyncio fake servers:

```sh
python3 -m unittest discover -s tests -v
```
