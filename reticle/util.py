from datetime import datetime
from typing import Type


class ExitCode(int):
    """The code returned to a parent process by an executable.

    Examples:
        >>> from subprocess import call
        >>> exit_code = ExitCode(call('ls'))
        >>> exit_code.is_ok()
        True

    """

    def __new__(cls, exit_code: int) -> Type['ExitCode']:
        """Make a new :class:`ExitCode`."""
        cls._code = exit_code
        return super().__new__(cls, exit_code)  # type: ignore

    def __add__(self, other: int) -> 'ExitCode':
        """Add an integer to this :class:`ExitCode`."""
        return ExitCode(self.__class__(super().__sub__(other)))

    def __sub__(self, other: int) -> 'ExitCode':
        """Subtract an integer from this :class:`ExitCode`."""
        return ExitCode(self.__class__(super().__sub__(other)))

    def is_ok(self) -> bool:
        """Is this code zero."""
        return self._code == 0

    def __repr__(self) -> str:
        """Represent this object."""
        return f'{self.__class__.__qualname__}({self._code})'


def format_ISO8601(moment: datetime) -> str:
    """Format a datetime into a filename compatible IS8601 representation.

    Args:
        moment: A datetime.

    Returns:
        The ISO8601 datetime formatted with hyphens as seperators.

    Examples:
        >>> from datetime import datetime
        >>> format_ISO8601(datetime(2018, 2, 23, 12, 13, 38))
        '2018-02-23T12-13-38'

    """
    return moment.strftime('%Y-%m-%dT%H-%M-%S')
