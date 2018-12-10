import io
import os
import sys
import shlex
from typing import Tuple

from awscli.clidriver import create_clidriver

from pendant.util import ExitCode

__all__ = ['awscli']


def awscli(command: str) -> Tuple[str, str]:
    """Use the ``awscli`` to execute a command.

    This function will call the ``awscli`` within the same process and not
    spawn subprocesses. In addition, the STDERR of the called function will be
    surpressed and the STDOUT will be returned as a string.

    Args:
        command: The command to be executed by the ``awscli``.

    Examples:
        >>> stdout, stderr = awscli('configure get region')
        >>> print(stdout.strip())
        us-west-2

    """
    cli_args = shlex.split(command)
    current_environment = dict(os.environ)
    stdout: str = ''
    stderr: str = ''
    current_stdout = sys.stdout
    current_stderr = sys.stderr
    try:
        env = os.environ.copy()
        env['LC_CTYPE'] = u'en_US.UTF'
        os.environ.update(env)

        sys.stdout = io.StringIO()
        sys.stderr = io.StringIO()

        driver = create_clidriver()
        exit_code = ExitCode(driver.main(cli_args))

        stdout = sys.stdout.getvalue()
        stderr = sys.stderr.getvalue()

        if not exit_code.is_ok():
            raise RuntimeError(f'AWS CLI exited with code {exit_code}\n{stdout}\n{stderr}')
    finally:
        sys.stdout = current_stdout
        sys.stderr = current_stderr
        os.environ.clear()
        os.environ.update(current_environment)
    return stdout, stderr
