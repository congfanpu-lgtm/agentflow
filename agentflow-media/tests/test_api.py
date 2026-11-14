import pytest
from fastapi.testclient import TestClient

from app.main import app, get_fetcher, get_ocr, get_stats, get_store, get_transcriber
from app.stats import Stats
from app.store import Store
from tests.fakes import URL, CountingOcr, FakeTranscriber, fake_fetch

MISSING = "f" * 32


@pytest.fixture
def client(tmp_path):
    store, stats, transcriber = Store(tmp_path), Stats(), FakeTranscriber()
    app.dependency_overrides.update({
        get_store: lambda: store, get_stats: lambda: stats, get_fetcher: lambda: fake_fetch([]),
        get_transcriber: lambda: transcriber, get_ocr: lambda: CountingOcr("slide text long enough"),
    })
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_import_transcribe_and_read_back(client):
    meta = client.post("/media/import", json={"url": URL}).json()
    key = meta["mediaKey"]
    assert meta["cached"] is False and meta["title"] == "Lecture 1"
    r = client.post(f"/media/{key}/transcribe")
    assert r.status_code == 200 and r.json()["utterances"] == 2 and r.json()["model"] == "large-v3"
    assert client.get(f"/media/{key}/transcript").json()["utterances"][0]["text"] == "Welcome to lecture one."
    assert client.get(f"/media/{key}/meta").json()["durationMs"] == 3600500
    assert client.get("/stats").json() == {"downloads": 1, "asrRuns": 1, "ocrRuns": 0}


def test_doing_it_all_again_does_no_work(client):
    key = client.post("/media/import", json={"url": URL}).json()["mediaKey"]
    client.post(f"/media/{key}/transcribe")
    assert client.post("/media/import", json={"url": URL}).json()["cached"] is True
    assert client.post(f"/media/{key}/transcribe").json()["cached"] is True
    assert client.get("/stats").json() == {"downloads": 1, "asrRuns": 1, "ocrRuns": 0}


def test_import_needs_exactly_one_of_url_or_path(client):
    assert client.post("/media/import", json={}).status_code == 422
    assert client.post("/media/import", json={"url": URL, "path": "/x"}).status_code == 422


def test_disallowed_url_is_400(client):
    assert client.post("/media/import", json={"url": "https://evil.example/x"}).status_code == 400


def test_download_failure_is_502(client):
    def broken(url, dest):
        raise RuntimeError("HTTP 403")
    app.dependency_overrides[get_fetcher] = lambda: broken
    r = client.post("/media/import", json={"url": URL})
    assert r.status_code == 502 and "detail" in r.json()


def test_unknown_or_malformed_key(client):
    assert client.get(f"/media/{MISSING}/meta").status_code == 404
    assert client.post(f"/media/{MISSING}/transcribe").status_code == 404
    assert client.post(f"/media/{MISSING}/frames").status_code == 404
    assert client.get("/media/NOT-A-KEY/meta").status_code == 400


def test_products_that_do_not_exist_yet_are_404(client):
    key = client.post("/media/import", json={"url": URL}).json()["mediaKey"]
    assert client.get(f"/media/{key}/transcript").status_code == 404
    assert client.get(f"/media/{key}/frames").status_code == 404


def test_frame_file_name_is_validated(client):
    key = client.post("/media/import", json={"url": URL}).json()["mediaKey"]
    assert client.get(f"/media/{key}/frames/meta.json").status_code == 400
    assert client.get(f"/media/{key}/frames/..%2Fmeta.json").status_code in (400, 404)
    assert client.get(f"/media/{key}/frames/000000000.jpg").status_code == 404


def test_health(client):
    assert client.get("/health").json() == {"status": "ok", "asrModel": "large-v3"}


def test_internal_failure_is_500_with_detail(tmp_path):
    store = Store(tmp_path)
    app.dependency_overrides.update({
        get_store: lambda: store, get_stats: lambda: Stats(), get_fetcher: lambda: fake_fetch([]),
        get_transcriber: lambda: FakeTranscriber(fail=True),
    })
    try:
        c = TestClient(app, raise_server_exceptions=False)
        key = c.post("/media/import", json={"url": URL}).json()["mediaKey"]
        r = c.post(f"/media/{key}/transcribe")
        assert r.status_code == 500 and "CUDA out of memory" in r.json()["detail"]
    finally:
        app.dependency_overrides.clear()


def test_cache_hit_and_unknown_key_do_not_load_the_model(client, monkeypatch):
    key = client.post("/media/import", json={"url": URL}).json()["mediaKey"]
    client.post(f"/media/{key}/transcribe")              # 用假转写器跑一次,留下缓存
    loaded = []
    monkeypatch.setattr("app.main.WhisperTranscriber", lambda *a: loaded.append(a))
    del app.dependency_overrides[get_transcriber]        # 之后走真实的依赖
    assert client.post(f"/media/{key}/transcribe").json()["cached"] is True
    assert client.post(f"/media/{MISSING}/transcribe").status_code == 404
    assert loaded == []
