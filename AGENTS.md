# AGENTS.md

Guidance for agents working on this repository.

## Project overview

`webdl-bridge` is a single-file Python (stdlib-only) Stremio addon that bridges Jackett torznab **hoster links** (1fichier, Mega, MediaFire, MegaUp, …) into playable streams via the **TorBox web download API**, with server-side `.rar`/`.zip` extraction.

Single source file: `server.py` (~550 lines). No dependencies beyond Python 3.12 stdlib + `bsdtar` (libarchive) in the container.

## Architecture

- `GET /stream/<type>/<id>.json` — resolves metadata via Cinemeta, queries the configured Jackett torznab indexers **in parallel** (`ThreadPoolExecutor`, hard `TORZNAB_TIMEOUT`), filters links against TorBox's supported-hosters allowlist (6h cache), caches results `STREAM_TTL` (15min), and fires background `createwebdownload` prefetch jobs.
- `GET /r/<b64url>` — resolve flow: `_links` map (persisted in `data/links.json`) → `ensure_webdl` → poll `mylist` until `download_finished`/`download_present` → playable file → 302 to `requestdl?...&redirect=true` permalink; archive → `download_and_extract` (CDN download + `bsdtar`) → serve extracted file with `Range` support.
- Job dedup: `resolve_async` shares one in-flight resolve per link via `_jobs` Events; entries are removed on completion so retries run fresh.
- Extraction: `bsdtar` (NOT `unrar`/`unar`/`7z` — Alpine's `7z` package has no RAR module; `unrar` isn't packaged; `unar` pulls a huge GNUstep tree on Debian).
- TorBox auth is **Bearer only** — query-param `?token=` returns 401 on `mylist`/`createwebdownload`. The `requestdl?...&token=&redirect=true` permalink works because it's the documented download-URL flow consumed by the client.
- State: `data/links.json` (link → `web_id` / extracted filename), `data/extracted/<web_id>/`.

## Constraints / conventions

- `/stream` must stay fast: AIOStreams kills slow addons. Keep parallel queries + cache; never block on TorBox in the stream path.
- No dependencies — stdlib only. Don't add requirements.txt/Flask; `ThreadingHTTPServer` is intentional.
- **Never commit `.env` or `data/`** — they contain the TorBox API key and downloaded/extracted media.
- One commit per functional change.

## Commands

```bash
docker compose build webdl-bridge && docker compose up -d --force-recreate webdl-bridge
docker logs -f webdl-bridge
curl -s http://localhost:4700/healthz
```

After editing `server.py`, a recreate is enough (file is bind-mounted read-only in the deploy compose).

## Known limits

- TorBox-only backend; MegaUp links currently fail at `createwebdownload` (TorBox-side resolver bug: "Request URL is missing an 'http://' or 'https://' protocol").
- Dead source links only work when TorBox has them cached.
- TorBox does not extract archives — that's why extraction lives here.
