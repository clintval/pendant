import signal
import time
from datetime import datetime
from typing import Any, Callable, Optional, Type

__all__ = ['ExitCode', 'timeout_after', 'wait_until', 'format_ISO8601']


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

    def is_ok(self) -> bool:
        """Is this code zero."""
        return self._code == 0

    def __repr__(self) -> str:
        """Represent this object."""
        return f'{self.__class__.__qualname__}({self._code})'


class timeout_after(object):
    """Context for timing out work if not done within a certain amount of time.

    Args:
        seconds: The number of seconds before raising execption.
        error_message: The message to raise execption with.

    Raises:
        TimeoutError: When work is not completed within a certain amount of time.

    Examples:
        >>> import time
        >>> with timeout_after(1):
        ...     time.sleep(0.5)
        ...     print(repr('yes!'))
        'yes!'

        >>> with timeout_after(1, 'I took too long!'):
        ...     time.sleep(2)
        ...     print(repr('no!'))
        Traceback (most recent call last):
            ...
        TimeoutError: I took too long!

    """

    def __init__(
        self, seconds: Optional[int] = None, error_message: str = 'A timeout has occured!'
    ) -> None:
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum: Any, frame: Any) -> None:
        """Raise :class:`TimeOutError` and return an error message."""
        raise TimeoutError(self.error_message)

    def __enter__(self) -> None:
        """Set a signal to raise an alarm after a number of seconds."""
        if self.seconds is not None:
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.alarm(self.seconds)

    def __exit__(self, type_: Any, value: Any, traceback: Any) -> None:
        """Turn off the alarm when we exit."""
        if self.seconds is not None:
            signal.alarm(0)


class wait_until(object):
    """Wait until a callable evaluates to a truthy statement, then continue.
    
    This object can be used as a function or as a context manager.

    Args:
        callable: A function which should eventually return a truthy value.
        call_rate: The rate at which to test the callable, optional.

    Examples:
        >>> iterable = [False, True, True, False, False]
        >>> with wait_until(iterable.pop):
        ...     print(iterable)
        [False, True]
        
        >>> # Trivial example of waiting a quarter of a second
        >>> wait_until(lambda: time.sleep(0.25) or 1)
        >>> print(repr('done!'))
        'done!'        

    """

    def __init__(self, statement: Callable, call_rate: Optional[int] = None) -> None:
        self.statement = statement
        self.call_rate = call_rate
        while not self.statement():
            if self.call_rate is not None:
                time.sleep(self.call_rate)

    def __enter__(self) -> None:
        """Do nothing on enter."""
        pass

    def __exit__(self, type_: Any, value: Any, traceback: Any) -> None:
        """Do nothing on exit."""
        pass


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
