"""On-disk layout: media/<key>/ (media + derived files), by-url/<url hash> (text file holding a key),
tmp/<random>/ (downloads in progress), inbox/ (local files waiting to be imported)."""
import hashlib
import json
import os
import re
import shutil
import tempfile
import threading
import uuid
from pathlib import Path

_KEY = re.compile(r"[0-9a-f]{32}")


def file_key(path: Path) -> str:
    with open(path, "rb") as f:
        return hashlib.file_digest(f, "sha256").hexdigest()[:32]


def _write_atomic(path: Path, text: str) -> None:
    """A crash or a full disk must never leave a half-written file that later reads as a cache hit."""
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        tmp.write_text(text, encoding="utf-8")
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


class Store:
    def __init__(self, root: Path):
        self.root = Path(root)
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    def dir(self, key: str) -> Path:
        # the key ends up in a filesystem path and arrives from HTTP: anything but 32 hex is rejected
        if not isinstance(key, str) or not _KEY.fullmatch(key):
            raise ValueError(f"bad media key: {key!r}")
        return self.root / "media" / key

    def exists(self, key: str) -> bool:
        return self.dir(key).is_dir()

    def read_json(self, key: str, name: str) -> dict | None:
        try:
            return json.loads((self.dir(key) / name).read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None

    def write_json(self, key: str, name: str, data: dict) -> None:
        d = self.dir(key)
        d.mkdir(parents=True, exist_ok=True)
        _write_atomic(d / name, json.dumps(data, ensure_ascii=False))

    def _url_file(self, url: str) -> Path:
        return self.root / "by-url" / hashlib.sha256(url.encode()).hexdigest()[:32]

    def url_key(self, url: str) -> str | None:
        try:
            return self._url_file(url).read_text().strip()
        except FileNotFoundError:
            return None

    def remember_url(self, url: str, key: str) -> None:
        f = self._url_file(url)
        f.parent.mkdir(parents=True, exist_ok=True)
        _write_atomic(f, key)

    def new_tmp(self) -> Path:
        (self.root / "tmp").mkdir(parents=True, exist_ok=True)
        return Path(tempfile.mkdtemp(dir=self.root / "tmp"))

    def adopt(self, tmp_dir: Path, key: str) -> bool:
        """Move a finished download into place. False = this content is already stored (tmp_dir is dropped)."""
        dest = self.dir(key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if not dest.exists():
            try:
                os.rename(tmp_dir, dest)
                return True
            except OSError:
                if not dest.exists():
                    raise
        shutil.rmtree(tmp_dir)
        return False

    def lock(self, name: str) -> threading.Lock:
        # ponytail: one lock per name, never evicted. Fine for a personal library (thousands of names);
        # switch to a weak-value dict if this ever serves an unbounded key space.
        with self._guard:
            return self._locks.setdefault(name, threading.Lock())
