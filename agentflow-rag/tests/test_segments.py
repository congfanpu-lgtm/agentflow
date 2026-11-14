import random

import pytest
from fastapi.testclient import TestClient

from app import segments
from app.main import app


def seg(start_s, text, kind="speech", dur_s=60):
    return {"startMs": start_s * 1000, "endMs": (start_s + dur_s) * 1000, "kind": kind, "text": text}


LECTURE_1 = [seg(0, "Welcome. Today we cover pre-training of large language models."),
             seg(60, "Gradient descent updates the parameters using the gradient of the loss."),
             seg(120, "Reinforcement learning from human feedback aligns the model.")]


@pytest.fixture
def course(db):
    segments.init()
    # 每个用例一门新课,检索互不影响。讲次 id 各用例是重复的(都从 1 起):index 按讲次整讲替换,
    # 后一个用例写入时会把前一个用例那一讲的片段删掉,所以不用清表
    return random.randrange(10 ** 6, 10 ** 9)


def test_search_returns_the_segment_with_its_time(course):
    assert segments.index(course, 1, LECTURE_1) == 3
    hit = segments.search(course, "how does gradient descent work", 1)[0]
    assert hit["id"] == "1:speech:60000" and hit["lectureId"] == 1 and hit["kind"] == "speech"
    assert hit["startMs"] == 60000 and hit["endMs"] == 120000 and "Gradient descent" in hit["text"]


@pytest.mark.parametrize("mode", ["hybrid", "dense", "bm25"])
def test_search_never_crosses_courses(course, mode):
    other = course + 1
    segments.index(course, 1, LECTURE_1)
    segments.index(other, 9, [seg(0, "Gradient descent, explained in the other course.")])
    hits = segments.search(course, "gradient descent", 10, mode=mode)
    assert hits and {h["lectureId"] for h in hits} == {1}
    assert {h["lectureId"] for h in segments.search(other, "gradient descent", 10, mode=mode)} == {9}


def test_search_can_be_limited_to_some_lectures(course):
    segments.index(course, 1, LECTURE_1)
    segments.index(course, 2, [seg(0, "Stochastic gradient descent uses one sample per step.")])
    assert {h["lectureId"] for h in segments.search(course, "gradient descent", 10)} == {1, 2}
    assert {h["lectureId"] for h in segments.search(course, "gradient descent", 10, lecture_ids=[2])} == {2}


def test_reindex_replaces_the_lecture(course):
    segments.index(course, 1, [seg(0, "Kafka partitions are rebalanced across consumers.")])
    # 新的一版从另一个时间点开始,片段 id 不同:只靠"同 id 覆盖"是清不掉旧片段的,必须先删这一讲
    assert segments.index(course, 1, [seg(30, "Redis runs Lua scripts atomically.")]) == 1
    for mode in ("bm25", "dense", "hybrid"):
        hits = segments.search(course, "kafka partitions rebalanced", 10, mode=mode)
        assert all("Kafka" not in h["text"] for h in hits), mode
    assert len(segments.search(course, "redis lua", 10)) == 1


def test_an_empty_list_clears_the_lecture(course):
    segments.index(course, 1, LECTURE_1)
    assert segments.index(course, 1, []) == 0 and segments.search(course, "gradient descent", 10) == []


def test_a_course_without_segments_has_no_hits(course):
    assert segments.search(course, "anything", 5) == []


def test_slide_and_speech_at_the_same_second_are_both_kept(course):
    segments.index(course, 1, [seg(60, "The speaker explains attention."),
                               seg(60, "Attention is all you need", kind="slide")])
    assert {h["id"] for h in segments.search(course, "attention", 10)} == {"1:speech:60000", "1:slide:60000"}


def test_lexical_index_is_rebuilt_from_the_table(course):
    segments.index(course, 1, LECTURE_1)
    segments.init()                                  # 等于服务重启
    assert segments.search(course, "reinforcement learning", 1, mode="bm25")[0]["startMs"] == 120000


def test_http_index_and_search(course):
    with TestClient(app) as c:
        r = c.post("/segments/index", json={"courseId": course, "lectureId": 1, "segments": LECTURE_1})
        assert r.status_code == 200 and r.json() == {"indexed": 3}
        hits = c.post("/segments/search", json={"courseId": course, "query": "pre-training", "topK": 2}).json()["hits"]
        assert hits[0]["startMs"] == 0 and set(hits[0]) == {"id", "lectureId", "startMs", "endMs", "kind", "text", "score"}
        bad = {"courseId": course, "lectureId": 1, "segments": [seg(0, "x", kind="video")]}
        assert c.post("/segments/index", json=bad).status_code == 422
        assert c.post("/segments/search", json={"courseId": course, "query": "x", "topK": 51}).status_code == 422
