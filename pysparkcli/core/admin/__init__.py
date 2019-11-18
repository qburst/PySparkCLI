import os
import sys


class CommandUtility:
    """
    Logic for building commands pysparkcli utilities.
    """
    def __init__(self, argv=None):
        self.argv = argv or sys.argv[:]
        self.prog_name = os.path.basename(self.argv[0])
        if self.prog_name == '__main__.py':
            self.prog_name = 'python -m django'
        self.settings_exception = None


def execute_from_command_line(argv=None):
    """Run a ManagementUtility."""
    print("{*}*40")