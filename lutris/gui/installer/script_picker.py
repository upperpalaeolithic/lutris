from gi.repository import GObject, Gtk

from lutris.gui.installer.script_box import InstallerScriptBox


class InstallerPicker(Gtk.ListBox):
    """List box to pick between several installers"""

    __gsignals__ = {"installer-selected": (GObject.SIGNAL_RUN_FIRST, None, (str,))}

    def __init__(self, scripts):
        super().__init__()
        revealed = True
        for script in scripts:
            self.add(InstallerScriptBox(script, parent=self, revealed=revealed))
            revealed = False  # Only reveal the first installer.
        self.connect("row-selected", self.on_activate)
        self.show_all()

    def set_platform_filter(self, runner):
        """Show only installers matching runner, or all installers when runner is None.

        Only meaningful when the game has multiple runner types available.
        """
        if runner is None:
            self.set_filter_func(None)
        else:
            self.set_filter_func(lambda row: row.get_children()[0].script.get("runner") == runner)
        self.invalidate_filter()

    @staticmethod
    def on_activate(widget, row):
        """Handler for hiding and showing the revealers in children"""
        for script_box_row in widget:
            script_box = script_box_row.get_children()[0]
            script_box.reveal(False)
        installer_row = row.get_children()[0]
        installer_row.reveal()
