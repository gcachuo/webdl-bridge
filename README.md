# webdl-bridge

Stremio addon that exposes **Jackett torznab hoster links** (1fichier, Mega, MediaFire, MegaUp, etc.) resolved through the **TorBox web download API**, including server-side extraction of `.rar`/`.zip` archives.

Built for setups where a Jackett indexer emits non-torrent releases (file hoster URLs) that Stremio/AIOStreams pipelines can't use: AIOStreams' torznab addon discards items without an infoHash, and neither its TorBox nor StremThru debrid integrations support web downloads.

## How it works

```
Stremio /stream request
  → queries Jackett torznab indexers (parallel, hard timeout)
  → filters links by TorBox's supported-hosters list
  → emits streams whose url points to /r/<base64 link>
Playback (/r/<link>)
  → creates (or reuses) a TorBox webdl
  → video file → 302 to TorBox CDN permalink
  → .rar/.zip → downloads from CDN, extracts with bsdtar,
    serves the video locally with Range support (cached on disk)
```

To stay within strict addon timeouts, `/stream` answers in ~10s (parallel queries + 15min result cache) and fires `createwebdownload` for each found link in the background, so TorBox is usually already done when the user picks a stream.

## API

| Endpoint | Description |
|---|---|
| `GET /manifest.json` | Stremio addon manifest (`stream` resource, `tt` ids) |
| `GET /stream/<movie|series>/<id>.json` | Streams; `id` = `tt0115958` or `tt1695360:4:1` |
| `GET /r/<b64url>` | Resolve a hoster link → 302 or extracted file |
| `GET /healthz` | Health check |

## Deploy

```bash
cp .env.example .env   # fill in TORBOX_API and JACKETT_APIKEY
docker compose up -d
```

Uses `network_mode: host` (matches media-server stacks where Jellyfin/AIOStreams run on the same host). Set `PUBLIC_BASE` to a reachable URL if clients aren't local.

Register `http://localhost:4700/manifest.json` as a custom addon in your Stremio/AIOStreams config.

## Configuration

See `.env.example`. Notables:

- `INDEXERS` — comma-separated Jackett indexer ids to query.
- `PREFETCH` — fire `createwebdownload` in the background when listing streams (recommended; costs TorBox API calls).
- `RESOLVE_TIMEOUT` — max wait for TorBox to fetch from the hoster (extraction time not included).
- Extracted files persist in `data/extracted/<web_id>/`; repeat plays are instant.

## Limits

- TorBox-only backend (no abstraction for other debrid services yet).
- Hosters not supported by TorBox are filtered out (see `/v1/api/webdl/hosters`).
- Links dead at the source only work if TorBox has them cached.
- First play of an uncached file can take minutes (hoster→TorBox→download→extract).
