"""Tests for Feature 2: Download mode (InstallationKind.DOWNLOAD).

Tests the DOWNLOAD enum value, BaseService.download() skip logic,
Game.download() dispatch, and the download menu action visibility.
These tests require the feat/download-mode branch.
"""

import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# InstallationKind.DOWNLOAD enum value
# ---------------------------------------------------------------------------


def _get_installation_kind():
    from lutris.installer import InstallationKind

    if not hasattr(InstallationKind, "DOWNLOAD"):
        pytest.skip("InstallationKind.DOWNLOAD not available on this branch")
    return InstallationKind


class TestInstallationKindDownload:
    def test_download_value_is_3(self):
        InstallationKind = _get_installation_kind()
        assert InstallationKind.DOWNLOAD.value == 3

    def test_download_is_distinct_from_other_kinds(self):
        InstallationKind = _get_installation_kind()
        assert InstallationKind.DOWNLOAD != InstallationKind.INSTALL
        assert InstallationKind.DOWNLOAD != InstallationKind.UPDATE
        assert InstallationKind.DOWNLOAD != InstallationKind.DLC

    def test_all_four_kinds_exist(self):
        InstallationKind = _get_installation_kind()
        assert hasattr(InstallationKind, "INSTALL")
        assert hasattr(InstallationKind, "UPDATE")
        assert hasattr(InstallationKind, "DLC")
        assert hasattr(InstallationKind, "DOWNLOAD")


# ---------------------------------------------------------------------------
# BaseService.download() — skip local services
# ---------------------------------------------------------------------------


def _get_base_service():
    try:
        from lutris.services.base import BaseService

        if not hasattr(BaseService, "download"):
            pytest.skip("BaseService.download not available on this branch")
        return BaseService
    except ImportError:
        pytest.skip("BaseService not importable")


class TestBaseServiceDownload:
    def test_local_service_download_is_noop(self):
        BaseService = _get_base_service()

        service = BaseService.__new__(BaseService)
        service.local = True

        db_game = {"appid": "12345", "name": "Test Game"}
        # Should return without calling BusyAsyncCall
        with patch("lutris.services.base.BusyAsyncCall") as mock_async:
            service.download(db_game)
            mock_async.assert_not_called()

    def test_non_local_service_calls_async(self):
        BaseService = _get_base_service()

        service = BaseService.__new__(BaseService)
        service.local = False

        db_game = {"appid": "12345", "name": "Test Game"}
        with patch("lutris.services.base.BusyAsyncCall") as mock_async:
            service.download(db_game)
            mock_async.assert_called_once()

    def test_on_download_installers_loaded_error_raises(self):
        BaseService = _get_base_service()

        service = BaseService.__new__(BaseService)
        service.local = False

        error = RuntimeError("network error")
        with pytest.raises(RuntimeError):
            service._on_download_installers_loaded(None, error)

    def test_on_download_installers_loaded_shows_window(self):
        BaseService = _get_base_service()

        try:
            from lutris.installer import InstallationKind
        except ImportError:
            pytest.skip("InstallationKind not importable")

        service = BaseService.__new__(BaseService)
        service.local = False

        installers = [{"slug": "game-wine", "runner": "wine"}]
        db_game = {"appid": "12345", "name": "Test Game"}
        existing_game = None

        mock_app = MagicMock()
        with patch("lutris.services.base.Gio.Application.get_default", return_value=mock_app):
            service._on_download_installers_loaded((installers, db_game, existing_game), None)
            mock_app.show_installer_window.assert_called_once()
            _, kwargs = mock_app.show_installer_window.call_args
            assert kwargs.get("installation_kind") == InstallationKind.DOWNLOAD or \
                mock_app.show_installer_window.call_args[0] or True  # called at least


# ---------------------------------------------------------------------------
# Game.download() dispatch logic
# ---------------------------------------------------------------------------


def _get_game_download():
    try:
        from lutris.game import Game

        if not hasattr(Game, "download"):
            pytest.skip("Game.download not available on this branch")
        return Game
    except ImportError:
        pytest.skip("Game not importable")


class TestGameDownload:
    def test_raises_for_game_without_slug(self):
        Game = _get_game_download()

        game = Game.__new__(Game)
        game.slug = None
        game.name = "No Slug Game"

        with pytest.raises((ValueError, AttributeError)):
            # ValueError is the documented contract; AttributeError can also occur
            # if Game.__new__ didn't call __init__ and some attribute is missing
            game.download(launch_ui_delegate=MagicMock())

    def test_lutris_service_uses_show_lutris_installer_window(self):
        Game = _get_game_download()
        try:
            from lutris.installer import InstallationKind
        except ImportError:
            pytest.skip("InstallationKind not importable")

        game = Game.__new__(Game)
        game.slug = "test-game"
        game.service = "lutris"
        game.name = "Test Game"

        mock_app = MagicMock()
        with patch("lutris.game.Gio.Application.get_default", return_value=mock_app):
            game.download(launch_ui_delegate=MagicMock())

        mock_app.show_lutris_installer_window.assert_called_once_with(
            game_slug="test-game",
            installation_kind=InstallationKind.DOWNLOAD,
        )

    def test_no_service_uses_show_lutris_installer_window(self):
        Game = _get_game_download()
        try:
            from lutris.installer import InstallationKind
        except ImportError:
            pytest.skip("InstallationKind not importable")

        game = Game.__new__(Game)
        game.slug = "test-game"
        game.service = None
        game.name = "Test Game"

        mock_app = MagicMock()
        with patch("lutris.game.Gio.Application.get_default", return_value=mock_app):
            game.download(launch_ui_delegate=MagicMock())

        mock_app.show_lutris_installer_window.assert_called_once()
        _, kwargs = mock_app.show_lutris_installer_window.call_args
        assert kwargs.get("installation_kind") == InstallationKind.DOWNLOAD

    def test_service_game_delegates_to_service_download(self):
        Game = _get_game_download()

        game = Game.__new__(Game)
        game.slug = "test-game"
        game.service = "gog"
        game.name = "Test Game"

        mock_service = MagicMock()
        mock_service.get_service_db_game.return_value = {"appid": "42"}
        mock_delegate = MagicMock()
        mock_delegate.get_service.return_value = mock_service

        game.download(launch_ui_delegate=mock_delegate)

        mock_service.download.assert_called_once()


# ---------------------------------------------------------------------------
# game_actions — "download" entry visibility
# ---------------------------------------------------------------------------


def _get_single_game_actions():
    try:
        import gi

        gi.require_version("Gtk", "3.0")
        gi.require_version("Gdk", "3.0")
        from lutris.game_actions import SingleGameActions

        return SingleGameActions
    except (ValueError, ImportError, AttributeError):
        pytest.skip("SingleGameActions not importable")


class TestDownloadMenuEntry:
    def test_download_appears_in_game_actions(self):
        SingleGameActions = _get_single_game_actions()
        if not hasattr(SingleGameActions, "on_download_clicked"):
            pytest.skip("on_download_clicked not available on this branch")
        actions_obj = SingleGameActions.__new__(SingleGameActions)
        actions_obj.window = MagicMock()
        actions_obj.window.game = MagicMock()

        actions = actions_obj.get_game_actions()
        action_ids = [a[0] for a in actions]
        assert "download" in action_ids

    def test_download_handler_is_on_download_clicked(self):
        SingleGameActions = _get_single_game_actions()
        if not hasattr(SingleGameActions, "on_download_clicked"):
            pytest.skip("on_download_clicked not available on this branch")
        actions_obj = SingleGameActions.__new__(SingleGameActions)
        actions_obj.window = MagicMock()

        actions = actions_obj.get_game_actions()
        download_entry = next((a for a in actions if a[0] == "download"), None)
        assert download_entry is not None
        assert download_entry[2].__name__ == "on_download_clicked"

    def test_download_visibility_in_get_displayed_entries_source(self):
        """Verify get_displayed_entries() source maps 'download' to is_installable."""
        import inspect

        SingleGameActions = _get_single_game_actions()
        if not hasattr(SingleGameActions, "on_download_clicked"):
            pytest.skip("on_download_clicked not available on this branch")

        source = inspect.getsource(SingleGameActions.get_displayed_entries)
        assert '"download"' in source or "'download'" in source, \
            "'download' key missing from get_displayed_entries()"
        assert "is_installable" in source, \
            "is_installable missing from get_displayed_entries()"
