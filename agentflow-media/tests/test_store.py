import pytest

from app.store import Store, file_key

KEY = "0123456789abcdef0123456789abcdef"


def test_rejects_keys_that_are_not_32_hex(tmp_path):
    s = Store(tmp_path)
    for bad in ["", "abc", "../etc", KEY + "0", KEY.upper(), "g" * 32, KEY[:31] + "/"]:
        with pytest.raises(ValueError):
            s.dir(bad)


def test_json_roundtrip_and_missing(tmp_path):
    s = Store(tmp_path)
    assert s.read_json(KEY, "meta.json") is None and not s.exists(KEY)
    s.write_json(KEY, "meta.json", {"title": "讲座 1"})
    assert s.read_json(KEY, "meta.json") == {"title": "讲座 1"} and s.exists(KEY)


def test_write_json_is_atomic(tmp_path, monkeypatch):
    s = Store(tmp_path)
    s.write_json(KEY, "t.json", {"v": 1})

    def boom(*a, **k):
        raise OSError("disk full")

    monkeypatch.setattr("app.store.os.replace", boom)
    with pytest.raises(OSError):
        s.write_json(KEY, "t.json", {"v": 2})
    assert s.read_json(KEY, "t.json") == {"v": 1}
    assert [p.name for p in s.dir(KEY).iterdir()] == ["t.json"]   # 没留下临时文件


def test_file_key_is_content_hash(tmp_path):
    a, b = tmp_path / "a.bin", tmp_path / "b.bin"
    a.write_bytes(b"x" * 3_000_000)
    b.write_bytes(b"x" * 3_000_000)
    assert file_key(a) == file_key(b)
    Store(tmp_path).dir(file_key(a))   # 是合法 key,不抛异常
    b.write_bytes(b"y")
    assert file_key(a) != file_key(b)


def test_url_index(tmp_path):
    s = Store(tmp_path)
    assert s.url_key("https://youtu.be/abc") is None
    s.remember_url("https://youtu.be/abc", KEY)
    assert s.url_key("https://youtu.be/abc") == KEY and s.url_key("https://youtu.be/other") is None


def test_adopt_moves_once(tmp_path):
    s = Store(tmp_path)
    t1 = s.new_tmp()
    (t1 / "audio.m4a").write_bytes(b"a")
    assert s.adopt(t1, KEY) is True
    assert (s.dir(KEY) / "audio.m4a").read_bytes() == b"a" and not t1.exists()
    t2 = s.new_tmp()
    (t2 / "audio.m4a").write_bytes(b"b")
    assert s.adopt(t2, KEY) is False                      # 已有:不覆盖,临时目录删掉
    assert (s.dir(KEY) / "audio.m4a").read_bytes() == b"a" and not t2.exists()


def test_lock_is_per_name(tmp_path):
    s = Store(tmp_path)
    assert s.lock("a") is s.lock("a") and s.lock("a") is not s.lock("b")
