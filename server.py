#!/usr/bin/env python3
"""webdl-bridge: Stremio addon that exposes Jackett torznab hoster links
resolved through the TorBox webdl API (with server-side rar/zip extraction).

Endpoints:
  GET /manifest.json
  GET /stream/<type>/<id>.json   (id = tt0115958 or tt1695360:4:1)
  GET /r/<b64url>                (302 -> TorBox CDN link or extracted file)
  GET /f/<web_id>                (serve extracted video, supports Range)
  GET /healthz

Latency budget: /stream answers within ~12s always (AIOStreams kills slow
addons). Torznab queries run in parallel with hard timeouts and results are
cached 15min. Hosters found are prefetched to TorBox in the background so the
resolve step usually hits an already-downloaded file.
"""

import base64
import concurrent.futures
import http.cookiejar
import json
import os
import re
import shutil
import subprocess
import threading
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "4700"))
TORBOX_API = os.environ["TORBOX_API"]
JACKETT_URL = os.environ.get("JACKETT_URL", "http://localhost:9118").rstrip("/")
JACKETT_APIKEY = os.environ["JACKETT_APIKEY"]
INDEXERS = [i.strip() for i in os.environ.get("INDEXERS", "lamovie").split(",") if i.strip()]
PUBLIC_BASE = os.environ.get("PUBLIC_BASE", "http://localhost:4700").rstrip("/")
RESOLVE_TIMEOUT = int(os.environ.get("RESOLVE_TIMEOUT", "300"))
DATA_DIR = os.environ.get("DATA_DIR", "/app/data")
STATE_FILE = os.path.join(DATA_DIR, "links.json")
TORZNAB_TIMEOUT = int(os.environ.get("TORZNAB_TIMEOUT", "10"))
STREAM_TTL = int(os.environ.get("STREAM_TTL", "900"))
PREFETCH = os.environ.get("PREFETCH", "true").lower() == "true"
UA = "webdl-bridge/1.0"

PLAYABLE = (".mkv", ".mp4", ".avi", ".m4v", ".ts", ".mov", ".wmv", ".webm")
ARCHIVE = (".rar", ".zip", ".7z", ".tar", ".tar.gz", ".tgz", ".001")

# Link-gate domains resolved here instead of via TorBox allowlist.
# todopaste /go/<id>: countdown gate -> POST /activity heartbeats -> /complete
RESOLVER_HOSTS = {"todopaste.com"}
GATE_TIMEOUT = int(os.environ.get("GATE_TIMEOUT", "90"))

_links = {}          # link -> {"web_id": int, "file": name}
_jobs = {}           # link -> threading.Event (in-flight resolve)
_stream_cache = {}   # req_id -> (ts, streams)
_lock = threading.Lock()
_pool = concurrent.futures.ThreadPoolExecutor(max_workers=8)


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def http_get(url, timeout=30, headers=None):
    req = urllib.request.Request(url, headers={"User-Agent": UA, **(headers or {})})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def http_get_noredirect(url, timeout=30):
    class NoRedir(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *a):
            return None

    op = urllib.request.build_opener(NoRedir)
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    try:
        r = op.open(req, timeout=timeout)
        r.close()
        return None
    except urllib.error.HTTPError as e:
        if e.code in (301, 302, 303, 307, 308):
            return e.headers.get("Location")
        raise


def http_post_form(url, fields, timeout=60):
    boundary = "----wdlb" + str(int(time.time() * 1000))
    body = b""
    for k, v in fields.items():
        body += f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode()
    body += f"--{boundary}--\r\n".encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "User-Agent": UA,
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Authorization": f"Bearer {TORBOX_API}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            detail = e.read().decode()[:300]
        except Exception:
            detail = ""
        raise RuntimeError(f"HTTP {e.code}: {detail or e.reason}")


def load_links():
    global _links
    try:
        with open(STATE_FILE) as f:
            _links = json.load(f)
    except Exception:
        _links = {}


def save_links():
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(_links, f)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log(f"save_links error: {e}")


def b64e(s):
    return base64.urlsafe_b64encode(s.encode()).decode().rstrip("=")


def b64d(s):
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)).decode()


def ext_of(path):
    p = re.sub(r"[?#].*$", "", urllib.parse.urlparse(path).path.lower())
    for e in ARCHIVE + PLAYABLE:
        if p.endswith(e):
            return e
    return os.path.splitext(p)[1]


def cinemeta_meta(media_type, imdb_id):
    """Resolve title/year via TMDB /find (imdb external id)."""
    api = os.environ.get("TMDB_API", "")
    if api:
        try:
            data = json.loads(http_get(
                f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={api}&external_source=imdb_id&language=es-MX",
                timeout=8))
            results = (data.get("tv_results") or []) if media_type == "series" else (data.get("movie_results") or [])
            if results:
                r = results[0]
                name = r.get("name") or r.get("title") or ""
                orig = r.get("original_name") or r.get("original_title") or ""
                date = r.get("first_air_date") or r.get("release_date") or ""
                return {"name": name, "original_name": orig, "year": date[:4]}
        except Exception as e:
            log(f"tmdb find failed for {imdb_id}: {e}")
    try:
        d = json.loads(http_get(
            f"https://v3-cinemeta.strem.io/meta/{'series' if media_type == 'series' else 'movie'}/{imdb_id}.json",
            timeout=8))
        return d.get("meta") or {}
    except Exception as e:
        log(f"cinemeta failed for {imdb_id}: {e}")
        return {}


def torznab_items(indexer, params):
    qs = {"apikey": JACKETT_APIKEY, "extended": "1", "limit": "100", **params}
    url = f"{JACKETT_URL}/api/v2.0/indexers/{indexer}/results/torznab/api?{urllib.parse.urlencode(qs)}"
    try:
        xml = http_get(url, timeout=TORZNAB_TIMEOUT)
    except Exception as e:
        log(f"torznab {indexer} {params.get('q','')!r} failed: {e}")
        return []
    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return []
    items = []
    for it in root.iter("item"):
        def tag(name):
            el = it.find(name)
            return (el.text or "").strip() if el is not None and el.text else ""

        enc_url = ""
        enc = it.find("enclosure")
        if enc is not None and "magnet:" not in (enc.get("url") or ""):
            enc_url = enc.get("url") or ""
        link = enc_url or tag("link")
        if not link.startswith("http") or "magnet:" in link:
            continue
        e = ext_of(link)
        if e in ARCHIVE:
            kind = "archive"
        elif e in PLAYABLE or not e:
            kind = "video"   # hoster page URLs without extension: let TorBox decide
        else:
            continue
        items.append({"title": tag("title"), "link": link, "size": tag("size"), "kind": kind, "indexer": indexer})
    return items


def host_of(url):
    h = urllib.parse.urlparse(url).netloc.lower()
    return h[4:] if h.startswith("www.") else h


_hosters = {"domains": set(), "ts": 0}


def supported_host(url):
    """Only emit hosters TorBox can actually resolve (allowlist, 6h cache)."""
    if time.time() - _hosters["ts"] > 21600 or not _hosters["domains"]:
        try:
            data = json.loads(
                http_get(
                    "https://api.torbox.app/v1/api/webdl/hosters",
                    timeout=15,
                    headers={"Authorization": f"Bearer {TORBOX_API}"},
                )
            )
            domains = set()
            for h in data.get("data") or []:
                if h.get("status"):
                    for d in h.get("domains") or []:
                        domains.add(d.lower().lstrip("www."))
            _hosters["domains"] = domains
            _hosters["ts"] = time.time()
            log(f"hosters allowlist: {len(domains)} domains")
        except Exception as e:
            log(f"hosters fetch failed: {e}")
            if not _hosters["domains"]:
                return True  # fail open
    host = host_of(url)
    return any(host == d or host.endswith("." + d) for d in _hosters["domains"])


def is_resolver(url):
    host = host_of(url)
    return any(host == d or host.endswith("." + d) for d in RESOLVER_HOSTS)


def resolve_gate(url):
    """Emulate a link-gate countdown (todopaste /go/<id>) -> real destination URL."""
    cj = http.cookiejar.CookieJar()
    op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))

    def greq(u, data=None, headers=None):
        r = urllib.request.Request(u, data=data, headers={"User-Agent": UA, **(headers or {})})
        return op.open(r, timeout=30)

    html = greq(url).read().decode("utf8", "replace")
    tok = re.search(r'session_token":"([0-9A-Za-z]{64})"', html)
    csrf = re.search(r'csrf-token" content="([^"]+)"', html)
    act = re.search(r'endpoint = "([^"]+activity)"', html)
    comp = re.search(r'completeUrl="([^"]+)"', html)
    if not (tok and csrf and act and comp):
        raise RuntimeError("gate page format not recognized")
    tok, csrf = tok.group(1), csrf.group(1)
    act = act.group(1).replace("\\/", "/")
    comp = comp.group(1).replace("\\/", "/")
    headers = {
        "Content-Type": "application/json",
        "X-CSRF-TOKEN": csrf,
        "X-Requested-With": "XMLHttpRequest",
    }
    deadline = time.time() + GATE_TIMEOUT
    while time.time() < deadline:
        body = json.dumps({"session_token": tok, "event": "heartbeat", "active_delta": 4}).encode()
        res = json.loads(greq(act, data=body, headers=headers).read())
        if res.get("can_complete"):
            out = json.loads(greq(comp, data=json.dumps({"session_token": tok}).encode(), headers=headers).read())
            dest = (out or {}).get("destination_url")
            if dest and dest.startswith("http"):
                log(f"gate resolved -> {dest[:80]}")
                return dest
            raise RuntimeError(f"gate complete rejected: {str(out)[:120]}")
        time.sleep(4)
    raise RuntimeError("gate timeout")


def fmt_size(n):
    try:
        n = int(n)
    except Exception:
        return ""
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or u == "TB":
            return f"{n}B" if u == "B" else f"{n:.1f}{u}"
        n /= 1024
    return ""


def prefetch(link):
    """Fire-and-forget: get the webdl created/downloading on TorBox early."""
    with _lock:
        entry = _links.get(link)
        if is_resolver(link) and entry and entry.get("gate_to"):
            real = entry["gate_to"]
            if real in _links:
                return
            _links[real] = {"web_id": None, "prefetch": True}
            link = real
        elif entry:
            return
        else:
            _links[link] = {"web_id": None, "prefetch": True}
    try:
        if is_resolver(link):
            real = resolve_gate(link)
            with _lock:
                _links[link] = {"gate_to": real, "gate_from": link}
                existing = _links.get(real)
                if existing and existing.get("web_id"):
                    save_links()
                    return
                _links[real] = {"web_id": None, "prefetch": True}
                save_links()
            link = real
        res = http_post_form("https://api.torbox.app/v1/api/webdl/createwebdownload", {"link": link})
        web_id = (res.get("data") or {}).get("webdownload_id")
        with _lock:
            if web_id:
                _links[link] = {"web_id": web_id}
                save_links()
                log(f"prefetch ok web_id={web_id} {link[:70]}")
            else:
                _links.pop(link, None)
                log(f"prefetch rejected {link[:70]}: {res.get('detail') or res.get('error')}")
    except Exception as e:
        with _lock:
            _links.pop(link, None)
        log(f"prefetch error {link[:70]}: {e}")


def streams_for(media_type, req_id):
    with _lock:
        hit = _stream_cache.get(req_id)
        if hit and time.time() - hit[0] < STREAM_TTL:
            return hit[1]

    parts = req_id.split(":")
    imdb_id = parts[0]
    season = int(parts[1]) if len(parts) > 1 else None
    episode = int(parts[2]) if len(parts) > 2 else None

    meta = cinemeta_meta(media_type, imdb_id)
    name = meta.get("name") or ""
    year = (meta.get("year") or "")[:4] or (meta.get("releaseInfo") or "")[:4]
    log(f"stream req {req_id} -> '{name}' {year} S{season}E{episode}")
    if not name:
        return []

    # Search original title first (site titles are often English), then localized
    orig = (meta.get("original_name") or "").strip()
    titles = [t for t in dict.fromkeys([orig, name]) if t]

    queries = []
    for indexer in INDEXERS:
        for t in titles:
            if media_type == "series" and season is not None:
                queries.append((indexer, {"t": "tvsearch", "q": t, "season": season, "ep": episode}))
            else:
                queries.append((indexer, {"t": "search", "q": f"{t} {year}".strip()}))
                queries.append((indexer, {"t": "search", "q": t}))

    seen, found = set(), []
    futures = [_pool.submit(torznab_items, ix, p) for ix, p in queries]
    for f in futures:
        try:
            res = f.result(timeout=TORZNAB_TIMEOUT + 5)
        except Exception:
            res = []
        for it in res:
            if it["link"] in seen or (not supported_host(it["link"]) and not is_resolver(it["link"])):
                continue
            seen.add(it["link"])
            found.append(it)

    def dest_entry(it):
        """Return (real_link, entry) for the torznab item, following gate_to."""
        with _lock:
            e = _links.get(it["link"]) or {}
            real = (e.get("gate_to") or it["link"]) if is_resolver(it["link"]) else it["link"]
            return real, _links.get(real) or {}

    def live_web_id(it):
        """web_id that still exists on TorBox (None purges stale entries)."""
        real, entry = dest_entry(it)
        web_id = entry.get("web_id")
        if not web_id:
            return None
        try:
            if torbox_mylist(web_id):
                return web_id
        except urllib.error.HTTPError as e:
            if e.code != 404:
                return web_id  # API hiccup: keep it, assume alive
        except Exception:
            return web_id  # API hiccup: keep it, assume alive
        with _lock:
            _links.pop(real, None)
            save_links()
        return None

    futures = [_pool.submit(live_web_id, it) for it in found]
    ready = [f.result() is not None for f in futures]
    # items already on TorBox first: mirrors needing a fresh download last
    found = [it for _, it in sorted(zip(ready, found), key=lambda t: not t[0])]
    log(f"found {len(found)} hoster items for '{name}'")

    streams = []
    for it in found:
        host = host_of(it["link"])
        size = fmt_size(it["size"])
        tag = "📦" if it["kind"] == "archive" else "🌐"
        streams.append(
            {
                "name": f"⚡WebDL {it.get('indexer','')}".strip(),
                "title": f"{it['title']}\n{tag}{host}" + (f" • 💾{size}" if size else ""),
                "url": f"{PUBLIC_BASE}/r/{b64e(it['link'])}",
                "behaviorHints": {"notWebReady": True},
            }
        )
        if PREFETCH and len(streams) == 1:
            # only warm the first item: the rest are usually mirrors of the same file
            _pool.submit(prefetch, it["link"])

    with _lock:
        _stream_cache[req_id] = (time.time(), streams)
        if len(_stream_cache) > 500:
            _stream_cache.clear()
    return streams


# ---------------- TorBox webdl ----------------

def torbox_mylist(web_id=None):
    qs = f"?id={web_id}" if web_id else ""
    data = json.loads(
        http_get(
            f"https://api.torbox.app/v1/api/webdl/mylist{qs}",
            timeout=20,
            headers={"Authorization": f"Bearer {TORBOX_API}"},
        )
    )
    d = data.get("data")
    if isinstance(d, list):
        return d[0] if d else None
    return d


def webdl_ready(item):
    if item.get("download_finished") or item.get("download_present"):
        return True
    state = str(item.get("download_state") or item.get("status") or "").lower()
    return any(s in state for s in ("completed", "cached", "seeding", "uploaded", "downloaded"))


def classify_files(item):
    files = item.get("files") or []
    playable, archives = [], []
    for f in files:
        e = ext_of(str(f.get("name") or f.get("short_name") or ""))
        if e in PLAYABLE:
            playable.append(f)
        elif e in ARCHIVE:
            archives.append(f)
    best = lambda l: max(l, key=lambda f: f.get("size") or 0) if l else None
    return best(playable), best(archives)


def requestdl_url(web_id, file_id=None, zip_link=False):
    return (
        "https://api.torbox.app/v1/api/webdl/requestdl"
        f"?token={urllib.parse.quote(TORBOX_API)}&web_id={web_id}"
        + (f"&file_id={file_id}" if file_id is not None else "")
        + ("&zip_link=true" if zip_link else "")
        + "&redirect=true"
    )


def extract_dir(web_id):
    return os.path.join(DATA_DIR, "extracted", str(web_id))


def download_and_extract(web_id, file_id, file_name=None):
    """Download the archive from TorBox CDN and extract the largest video."""
    out_dir = extract_dir(web_id)
    os.makedirs(out_dir, exist_ok=True)
    cdn = requestdl_url(web_id, file_id=file_id)
    real = http_get_noredirect(cdn, timeout=20) or cdn
    archive_path = os.path.join(out_dir, "src" + (ext_of(file_name or "") or ".rar"))
    log(f"downloading archive web_id={web_id}")
    req = urllib.request.Request(real, headers={"User-Agent": UA})
    with urllib.request.urlopen(req, timeout=600) as r, open(archive_path, "wb") as w:
        shutil.copyfileobj(r, w, 1024 * 1024)
    log(f"extracting {archive_path}")
    subprocess.run(["bsdtar", "-xf", archive_path, "-C", out_dir], check=True, timeout=600)
    os.remove(archive_path)
    best, best_size = None, 0
    for root, _, files in os.walk(out_dir):
        for fn in files:
            if ext_of(fn) in PLAYABLE:
                p = os.path.join(root, fn)
                s = os.path.getsize(p)
                if s > best_size:
                    best, best_size = p, s
    return best


def ensure_webdl(link):
    """Return (item, web_id); creates the webdl on TorBox if needed."""
    with _lock:
        web_id = (_links.get(link) or {}).get("web_id")
    if web_id:
        try:
            item = torbox_mylist(web_id)
            if item:
                return item, web_id
        except Exception:
            pass
    log(f"creating webdl for {link[:80]}")
    res = http_post_form("https://api.torbox.app/v1/api/webdl/createwebdownload", {"link": link})
    web_id = (res.get("data") or {}).get("webdownload_id")
    if not web_id:
        raise RuntimeError(res.get("detail") or res.get("error") or "rejected")
    with _lock:
        _links[link] = {"web_id": web_id}
        save_links()
    return None, web_id


def do_resolve(link):
    """Blocking resolve. Returns (kind, target): kind 'redirect'|'file'."""
    with _lock:
        entry = _links.get(link) or {}
    # Resolve link gates (e.g. todopaste /go/) to the real hoster URL first;
    # cache the mapping so replays skip the countdown.
    if is_resolver(link):
        if entry.get("gate_to"):
            link = entry["gate_to"]
        else:
            real = resolve_gate(link)
            with _lock:
                _links[link] = {"gate_to": real, "gate_from": link}
                save_links()
            link = real
        with _lock:
            entry = _links.get(link) or {}
    # already extracted?
    web_id = entry.get("web_id")
    if web_id and entry.get("file"):
        p = os.path.join(extract_dir(web_id), entry["file"])
        if os.path.exists(p):
            return "file", (web_id, entry["file"], p, os.path.getsize(p))

    item, web_id = ensure_webdl(link)
    deadline = time.time() + RESOLVE_TIMEOUT
    while True:
        if item is None:
            item = torbox_mylist(web_id)
        if item and webdl_ready(item):
            video, archive = classify_files(item)
            if video:
                return "redirect", requestdl_url(web_id, file_id=video.get("id"))
            if archive:
                path = download_and_extract(web_id, archive.get("id"), archive.get("name") or archive.get("short_name"))
                if not path:
                    return None, "no-video-in-archive"
                fn = os.path.basename(path)
                with _lock:
                    _links[link] = {"web_id": web_id, "file": fn}
                    save_links()
                return "file", (web_id, fn, path, os.path.getsize(path))
            if item.get("files"):
                return None, "unsupported"
        if time.time() > deadline:
            return None, "timeout"
        time.sleep(5)
        item = None


def resolve_async(link):
    """Dedup: concurrent resolves of the same link share one job."""
    with _lock:
        ev = _jobs.get(link)
        if ev is None:
            ev = threading.Event()
            _jobs[link] = ev
            ev.result = None
            owner = True
        else:
            owner = False
    if owner:
        def run():
            try:
                ev.result = do_resolve(link)
            except Exception as e:
                log(f"resolve error {link[:60]}: {e}")
                ev.result = (None, "error")
            finally:
                ev.set()
                with _lock:
                    _jobs.pop(link, None)
        _pool.submit(run)
    ev.wait(timeout=RESOLVE_TIMEOUT + 660)
    return ev.result or (None, "timeout")


MANIFEST = {
    "id": "community.gcachuo.webdl-bridge",
    "version": "1.1.0",
    "name": "WebDL Bridge (hosters via TorBox)",
    "description": "Jackett torznab hoster links resolved through TorBox web downloads (rar/zip extracted server-side).",
    "resources": ["stream"],
    "types": ["movie", "series"],
    "idPrefixes": ["tt"],
    "catalogs": [],
}


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body=b"", ctype="application/json", extra=None):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Access-Control-Allow-Origin", "*")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_GET(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/healthz":
            return self._send(200, b"ok", "text/plain")
        if path == "/manifest.json":
            return self._send(200, json.dumps(MANIFEST).encode())

        m = re.match(r"^/stream/(movie|series)/([^/]+)\.json$", path)
        if m:
            try:
                streams = streams_for(m.group(1), urllib.parse.unquote(m.group(2)))
            except Exception as e:
                log(f"streams_for error: {e}")
                streams = []
            return self._send(200, json.dumps({"streams": streams}).encode())

        m = re.match(r"^/r/([A-Za-z0-9_-]+)$", path)
        if m:
            try:
                link = b64d(m.group(1))
            except Exception:
                return self._send(400, b"bad link", "text/plain")
            log(f"resolve {link[:80]}")
            (kind, target) = resolve_async(link) or (None, "error")
            if kind == "redirect":
                return self._send(302, b"", "text/plain", {"Location": target})
            if kind == "file":
                web_id, fn, fpath, size = target
                return self._serve_file(web_id, fpath, fn, size)
            code = {"timeout": 503, "no-video-in-archive": 415, "unsupported": 415}.get(target, 502)
            return self._send(code, f"webdl resolve failed: {target}".encode(), "text/plain")

        m = re.match(r"^/f/(\d+)$", path)
        if m:
            web_id = m.group(1)
            for entry in _links.values():
                if entry.get("web_id") == int(web_id) and entry.get("file"):
                    fpath = os.path.join(extract_dir(web_id), entry["file"])
                    if os.path.exists(fpath):
                        return self._serve_file(web_id, fpath, entry["file"], os.path.getsize(fpath))
            return self._send(404, b"gone", "text/plain")

        log(f"404 path={self.path!r}")
        self._send(404, b"not found", "text/plain")

    def _serve_file(self, web_id, fpath, fn, size):
        range_h = self.headers.get("Range")
        start, end = 0, size - 1
        code = 200
        if range_h:
            rm = re.match(r"bytes=(\d*)-(\d*)", range_h)
            if rm:
                if rm.group(1):
                    start = int(rm.group(1))
                if rm.group(2):
                    end = min(int(rm.group(2)), size - 1)
                code = 206
        length = end - start + 1
        self.send_response(code)
        self.send_header("Content-Type", "video/x-matroska")
        self.send_header("Accept-Ranges", "bytes")
        self.send_header("Content-Length", str(length))
        if code == 206:
            self.send_header("Content-Range", f"bytes {start}-{end}/{size}")
        self.send_header("Content-Disposition", f'inline; filename="{fn}"')
        self.end_headers()
        with open(fpath, "rb") as f:
            f.seek(start)
            remaining = length
            while remaining > 0:
                chunk = f.read(min(1024 * 1024, remaining))
                if not chunk:
                    break
                try:
                    self.wfile.write(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    break
                remaining -= len(chunk)

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    load_links()
    log(f"webdl-bridge on :{PORT} indexers={INDEXERS} prefetch={PREFETCH}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
