"""Tests for Feature 1: platform filter in the installer picker.

Tests _preferred_runner() logic and set_platform_filter() on InstallerPicker.
These tests require the feat/prefer-native-platform-installer branch.
"""

import pytest

# Require GTK before any imports that need it
try:
    import gi

    gi.require_version("Gtk", "3.0")
    gi.require_version("Gdk", "3.0")
    from gi.repository import Gtk

    HAS_GTK = True
except (ValueError, ImportError):
    HAS_GTK = False

pytestmark = pytest.mark.skipif(not HAS_GTK, reason="GTK 3.0 not available")


# ---------------------------------------------------------------------------
# _preferred_runner — pure logic, no GTK needed
# ---------------------------------------------------------------------------


def _get_preferred_runner():
    """Import _preferred_runner, skipping if not present on this branch."""
    try:
        from lutris.gui.installerwindow import InstallerWindow

        return InstallerWindow._preferred_runner
    except (ImportError, AttributeError):
        pytest.skip("_preferred_runner not available on this branch")


class TestPreferredRunner:
    def test_returns_linux_when_both_linux_and_wine(self):
        preferred_runner = _get_preferred_runner()
        installers = [
            {"runner": "linux", "slug": "game-linux"},
            {"runner": "wine", "slug": "game-wine"},
        ]
        assert preferred_runner(installers) == "linux"

    def test_returns_none_for_single_linux_runner(self):
        preferred_runner = _get_preferred_runner()
        installers = [
            {"runner": "linux", "slug": "game-a"},
            {"runner": "linux", "slug": "game-b"},
        ]
        assert preferred_runner(installers) is None

    def test_returns_none_for_single_wine_runner(self):
        preferred_runner = _get_preferred_runner()
        installers = [
            {"runner": "wine", "slug": "game-wine-1"},
            {"runner": "wine", "slug": "game-wine-2"},
        ]
        assert preferred_runner(installers) is None

    def test_returns_none_when_no_linux_among_multiple_runners(self):
        preferred_runner = _get_preferred_runner()
        installers = [
            {"runner": "wine", "slug": "game-wine"},
            {"runner": "dosbox", "slug": "game-dosbox"},
        ]
        assert preferred_runner(installers) is None

    def test_returns_none_for_empty_list(self):
        preferred_runner = _get_preferred_runner()
        assert preferred_runner([]) is None

    def test_returns_none_for_single_installer(self):
        preferred_runner = _get_preferred_runner()
        installers = [{"runner": "linux", "slug": "game-only"}]
        assert preferred_runner(installers) is None

    def test_linux_preferred_with_three_runner_types(self):
        preferred_runner = _get_preferred_runner()
        installers = [
            {"runner": "linux", "slug": "native"},
            {"runner": "wine", "slug": "wine-build"},
            {"runner": "dosbox", "slug": "dosbox-build"},
        ]
        assert preferred_runner(installers) == "linux"

    def test_missing_runner_key_treated_as_none_runner(self):
        preferred_runner = _get_preferred_runner()
        installers = [
            {"runner": "linux", "slug": "native"},
            {"slug": "no-runner"},
        ]
        # Two distinct runner values (linux and None) — linux is preferred
        assert preferred_runner(installers) == "linux"


# ---------------------------------------------------------------------------
# InstallerPicker.set_platform_filter — GTK required
# ---------------------------------------------------------------------------


def _make_script_box(runner):
    """Create a mock row containing an InstallerScriptBox-like child."""
    try:
        from lutris.gui.installer.script_picker import InstallerScriptBox
    except ImportError:
        pytest.skip("InstallerScriptBox not importable")

    box = InstallerScriptBox({"runner": runner, "name": "Test", "slug": "test", "version": "1.0"})
    row = Gtk.ListBoxRow()
    row.add(box)
    return row


def _get_installer_picker():
    try:
        from lutris.gui.installer.script_picker import InstallerPicker

        if not hasattr(InstallerPicker, "set_platform_filter"):
            pytest.skip("set_platform_filter not available on this branch")
        return InstallerPicker
    except ImportError:
        pytest.skip("InstallerPicker not importable")


def _full_installer(runner, slug):
    return {
        "runner": runner,
        "name": "Test Game",
        "slug": slug,
        "version": "1.0",
        "description": "",
        "notes": "",
        "rating": "",
        "credits": "",
    }


class TestSetPlatformFilter:
    def test_filter_hides_wine_rows(self):
        InstallerPicker = _get_installer_picker()
        installers = [
            _full_installer("linux", "native"),
            _full_installer("wine", "wine-ver"),
        ]
        picker = InstallerPicker(installers)
        # Should not raise; GTK filter func is applied
        picker.set_platform_filter("linux")
        picker.set_platform_filter(None)

    def test_clear_filter_restores_all_rows(self):
        InstallerPicker = _get_installer_picker()
        installers = [
            _full_installer("linux", "native"),
            _full_installer("wine", "wine-ver"),
        ]
        picker = InstallerPicker(installers)
        picker.set_platform_filter("linux")
        picker.set_platform_filter(None)  # Should not raise

    def test_set_filter_does_not_raise_on_empty_list(self):
        InstallerPicker = _get_installer_picker()
        picker = InstallerPicker([])
        picker.set_platform_filter("linux")
        picker.set_platform_filter(None)
