# WebSocket Debug Tool

A standalone debug tool for connecting to Zero's WebSocket sync endpoint and capturing all messages.

## Features

- Connects to Zero sync WebSocket endpoints
- Decodes and displays connection metadata
- Pretty-prints all incoming messages with timestamps
- Shows message types with color highlighting
- Captures messages for a configurable duration (default: 5 seconds)

## Setup

```bash
# From the monorepo root
npm install
```

## Usage

### Quick Start

The tool comes pre-configured with a connection URL and headers. Simply run:

```bash
npm --workspace=ws-debug run debug
```

### Customizing the Connection

Edit `src/main.ts` to modify:

1. **WS_URL**: The WebSocket endpoint URL with query parameters
2. **SEC_WEBSOCKET_PROTOCOL**: The base64-encoded protocol header containing:
   - `initConnectionMessage`: Optional initial connection setup
   - `authToken`: Optional JWT authentication token
3. **CAPTURE_DURATION_MS**: How long to capture messages (default: 5000ms)

### Example Output

```
================================================================================
CONNECTION METADATA
================================================================================
URL:           ws://localhost:4848/sync/v40/connect?clientID=...
Client ID:     ku7aode2vofomjp8g9
Client Group:  ao5g82gvq6t033g3v6
User ID:       LlcOS6u-Dv5xBIihbE9UC
Base Cookie:   64oznwo8:06
Timestamp:     58467.700000047684
LMID:          0
WSID:          P-OGZHKQxgKT8NKlTdBtl

PROTOCOL HEADER:
Auth Token:    eyJhbGciOiJQUzI1NiJ9.eyJzdWIiOiJMbGNPUzZ1LUR2N...[300 more chars]
Init Message:   ["initConnection", {...}]
================================================================================

================================================================================
MESSAGES
================================================================================

[0.023s] connected
{
  "wsid": "P-OGZHKQxgKT8NKlTdBtl",
  "timestamp": 1234567890
}

[0.145s] pokeStart
{
  "baseCookie": "64oznwo8:06",
  "cookie": "64oznwo8:07",
  "schemaVersions": {...}
}

[0.201s] pokePart
{
  "rowsPatch": [...],
  "queriesPatch": [...]
}

================================================================================
SUMMARY: Captured 15 messages in 5s
================================================================================
```

## Extracting Connection Details from Browser

If you want to debug a connection from your browser's DevTools:

1. Open DevTools → Network tab → WS filter
2. Find the `/sync/v40/connect` WebSocket connection
3. Copy the connection URL and headers
4. In the Headers section, find:
   - Request URL → Use as `WS_URL`
   - `Sec-WebSocket-Protocol` → Use as `SEC_WEBSOCKET_PROTOCOL`
5. Update `src/main.ts` with these values

## Development

```bash
# Type checking
npm --workspace=ws-debug run check-types

# Linting
npm --workspace=ws-debug run lint

# Formatting
npm --workspace=ws-debug run format
```

## Protocol Details

The WebSocket sync protocol uses JSON messages in the format `[type, body]`:

**Common message types:**

- `connected` - Connection established
- `pokeStart` - Begin sync update
- `pokePart` - Partial sync data
- `pokeEnd` - End sync update
- `ping`/`pong` - Heartbeat
- `error` - Error occurred

See `packages/zero-protocol/src/` for full protocol definitions.
