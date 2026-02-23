from parsertang.config import Settings
from parsertang.utils.exchange_credentials import build_exchange_config


def test_htx_proxy_disabled_suppresses_socks_settings():
    settings = Settings(htx_proxy="OFF", _env_file=None)
    config = build_exchange_config("htx", settings)
    assert "socksProxy" not in config
    assert "wsSocksProxy" not in config


def test_htx_proxy_default_includes_socks_settings():
    settings = Settings(_env_file=None)
    config = build_exchange_config("htx", settings)
    assert "socksProxy" in config
    assert "wsSocksProxy" in config
