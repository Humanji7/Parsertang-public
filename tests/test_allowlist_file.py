import json


def test_load_allowlist_from_json_list(tmp_path):
    from parsertang.allowlist import load_allowlist

    path = tmp_path / "allow.json"
    path.write_text(json.dumps(["wif/usdt", "BTC/USDT", "  "]))
    assert load_allowlist(str(path)) == ["WIF/USDT", "BTC/USDT"]


def test_load_allowlist_from_json_object(tmp_path):
    from parsertang.allowlist import load_allowlist

    path = tmp_path / "allow.json"
    path.write_text(json.dumps({"symbols": ["wif/usdt", "ONDO/USDT"]}))
    assert load_allowlist(str(path)) == ["WIF/USDT", "ONDO/USDT"]


def test_load_allowlist_from_csv_text(tmp_path):
    from parsertang.allowlist import load_allowlist

    path = tmp_path / "allow.txt"
    path.write_text("wif/usdt, ondo/usdt\n")
    assert load_allowlist(str(path)) == ["WIF/USDT", "ONDO/USDT"]

