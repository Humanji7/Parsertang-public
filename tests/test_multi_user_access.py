"""Tests for multi-user access control (ACCESS_CONTROL_IDS)."""

import pytest
from unittest.mock import MagicMock, patch


class TestAccessControlIdsParsing:
    """Test Settings.get_access_control_ids() parsing."""

    def test_parse_json_array(self):
        """Should parse JSON array format."""
        from parsertang.config import Settings

        s = Settings(access_control_ids='["123456789","987654321"]')
        assert s.get_access_control_ids() == {"123456789", "987654321"}

    def test_parse_comma_separated(self):
        """Should parse comma-separated format."""
        from parsertang.config import Settings

        s = Settings(access_control_ids="123456789,987654321")
        assert s.get_access_control_ids() == {"123456789", "987654321"}

    def test_parse_single_value(self):
        """Should parse single value."""
        from parsertang.config import Settings

        s = Settings(access_control_ids="123456789")
        assert s.get_access_control_ids() == {"123456789"}

    def test_empty_string(self):
        """Should return empty set for empty string."""
        from parsertang.config import Settings

        s = Settings(access_control_ids="")
        assert s.get_access_control_ids() == set()

    def test_none_value(self):
        """Should return empty set for None."""
        from parsertang.config import Settings

        s = Settings(access_control_ids=None)
        assert s.get_access_control_ids() == set()

    def test_strips_whitespace(self):
        """Should strip whitespace from IDs."""
        from parsertang.config import Settings

        s = Settings(access_control_ids=" 123456789 , 987654321 ")
        assert s.get_access_control_ids() == {"123456789", "987654321"}

    def test_json_with_spaces(self):
        """Should handle JSON with extra spaces."""
        from parsertang.config import Settings

        s = Settings(access_control_ids='[ "123456789" , "987654321" ]')
        assert s.get_access_control_ids() == {"123456789", "987654321"}


class TestMultiUserAuthorization:
    """Test SimpleBot._authorized with multi-user access."""

    @pytest.fixture
    def mock_update(self):
        """Create mock Telegram update."""
        update = MagicMock()
        update.effective_chat = MagicMock()
        update.effective_chat.id = 123456789
        return update

    @pytest.mark.asyncio
    async def test_authorized_via_access_control_ids(self, mock_update):
        """Should authorize user in access_control_ids."""
        with patch("parsertang.alerts.settings") as mock_settings:
            mock_settings.get_access_control_ids.return_value = {
                "123456789",
                "999999999",
            }
            mock_settings.telegram_chat_id = "other_id"
            mock_settings.telegram_bot_token = "test_token"

            from parsertang.alerts import SimpleBot

            with patch.object(SimpleBot, "__init__", lambda x, trader=None: None):
                bot = SimpleBot()
                bot.trader = None

                result = await bot._authorized(mock_update)
                assert result is True

    @pytest.mark.asyncio
    async def test_unauthorized_not_in_access_control_ids(self, mock_update):
        """Should reject user not in access_control_ids."""
        with patch("parsertang.alerts.settings") as mock_settings:
            mock_settings.get_access_control_ids.return_value = {
                "999999999",
                "888888888",
            }
            mock_settings.telegram_chat_id = (
                "123456789"  # Would match fallback, but ignored
            )
            mock_settings.telegram_bot_token = "test_token"

            from parsertang.alerts import SimpleBot

            with patch.object(SimpleBot, "__init__", lambda x, trader=None: None):
                bot = SimpleBot()
                bot.trader = None

                result = await bot._authorized(mock_update)
                assert result is False

    @pytest.mark.asyncio
    async def test_fallback_to_telegram_chat_id(self, mock_update):
        """Should fallback to telegram_chat_id when access_control_ids empty."""
        with patch("parsertang.alerts.settings") as mock_settings:
            mock_settings.get_access_control_ids.return_value = set()  # Empty
            mock_settings.telegram_chat_id = "123456789"
            mock_settings.telegram_bot_token = "test_token"

            from parsertang.alerts import SimpleBot

            with patch.object(SimpleBot, "__init__", lambda x, trader=None: None):
                bot = SimpleBot()
                bot.trader = None

                result = await bot._authorized(mock_update)
                assert result is True

    @pytest.mark.asyncio
    async def test_fallback_rejects_wrong_id(self, mock_update):
        """Should reject when fallback chat_id doesn't match."""
        mock_update.effective_chat.id = 111111111  # Different ID

        with patch("parsertang.alerts.settings") as mock_settings:
            mock_settings.get_access_control_ids.return_value = set()  # Empty
            mock_settings.telegram_chat_id = "123456789"
            mock_settings.telegram_bot_token = "test_token"

            from parsertang.alerts import SimpleBot

            with patch.object(SimpleBot, "__init__", lambda x, trader=None: None):
                bot = SimpleBot()
                bot.trader = None

                result = await bot._authorized(mock_update)
                assert result is False
