from parsertang.config import settings


def test_alert_channel_routing_uses_trade_threshold_when_set(monkeypatch):
    monkeypatch.setattr(settings, "min_net_profit", -0.10, raising=False)
    monkeypatch.setattr(settings, "min_net_profit_trade", 0.10, raising=False)

    from parsertang.alerts import pick_alert_channel  # local import for patched settings

    assert pick_alert_channel(0.12) == "trade"
    assert pick_alert_channel(0.05) == "tech"

