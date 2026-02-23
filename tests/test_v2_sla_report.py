from parsertang.v2.sla_report import format_sla_report


def test_format_sla_report():
    text = format_sla_report(healthy_ratio=0.96, fresh_ratio_min=0.85)
    assert "96%" in text
    assert "0.85" in text
