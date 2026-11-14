#!/usr/bin/env python3
"""Smoke-test a running media service with one real lecture: import -> transcribe -> frames, twice.

The second pass must be all cache hits and must not move /stats; otherwise the exit status is 1.
Standard library only.   usage: smoke.py --base http://127.0.0.1:8010 --url <YouTube link>
"""
import argparse
import json
import sys
import time
import urllib.error
import urllib.request


def call(base: str, method: str, path: str, body: dict | None = None) -> tuple[dict, float]:
    req = urllib.request.Request(base + path, method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=1800) as r:
            return json.load(r), time.perf_counter() - t0
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path} -> {e.code}: {e.read().decode(errors='replace')}")


def one_pass(base: str, url: str):
    meta, t_import = call(base, "POST", "/media/import", {"url": url})
    key = meta["mediaKey"]
    asr, t_asr = call(base, "POST", f"/media/{key}/transcribe")
    frames, t_frames = call(base, "POST", f"/media/{key}/frames")
    return meta, asr, frames, (t_import, t_asr, t_frames)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8010")
    ap.add_argument("--url", required=True)
    a = ap.parse_args()

    meta, asr, frames, (t_import, t_asr, t_frames) = one_pass(a.base, a.url)
    stats_first, _ = call(a.base, "GET", "/stats")
    meta2, asr2, frames2, _ = one_pass(a.base, a.url)
    stats_after, _ = call(a.base, "GET", "/stats")
    all_cached = bool(meta2["cached"] and asr2["cached"] and frames2["cached"])

    print(json.dumps({
        "mediaKey": meta["mediaKey"], "title": meta["title"], "durationMs": meta["durationMs"],
        "importS": round(t_import, 1), "transcribeS": round(t_asr, 1),
        # speed only means something when this run did the transcription
        "realtimeX": None if asr["cached"] else round(meta["durationMs"] / 1000 / t_asr, 1),
        "utterances": asr["utterances"], "framesS": round(t_frames, 1), "frames": frames["frames"],
        "secondPassAllCached": all_cached, "statsAfter": stats_after,
    }, ensure_ascii=False))
    return 0 if all_cached and stats_first == stats_after else 1


if __name__ == "__main__":
    sys.exit(main())
