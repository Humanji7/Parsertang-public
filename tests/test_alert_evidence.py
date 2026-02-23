import json

from parsertang.alert_evidence import append_jsonl


def test_append_jsonl_writes_line(tmp_path):
    path = tmp_path / "evidence.jsonl"
    append_jsonl(str(path), {"a": 1, "b": "x"})
    text = path.read_text(encoding="utf-8").strip()
    assert text
    record = json.loads(text)
    assert record["a"] == 1
    assert record["b"] == "x"

