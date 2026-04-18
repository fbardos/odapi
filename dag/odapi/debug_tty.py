# odapi/debug_tty.py
from __future__ import annotations

import atexit
import os
import sys

try:
    import pdbpp as pdb_mod
except ImportError:
    import pdb as pdb_mod


class DagsterTtyPdb(pdb_mod.Pdb):
    def __init__(self, *args, **kwargs) -> None:
        if not os.path.exists("/dev/tty"):
            raise RuntimeError("No /dev/tty available for interactive debugging")

        self._tty_in = open("/dev/tty", "r")
        self._tty_out = open("/dev/tty", "w", buffering=1)
        self._closed = False

        kwargs.setdefault("stdin", self._tty_in)
        kwargs.setdefault("stdout", self._tty_out)

        super().__init__(*args, **kwargs)
        self.use_rawinput = True

    def interaction(self, frame, tb):
        old_stdin = sys.stdin
        old_stdout = sys.stdout
        old_stderr = sys.stderr
        try:
            sys.stdin = self._tty_in
            sys.stdout = self._tty_out
            sys.stderr = self._tty_out
            return super().interaction(frame, tb)
        finally:
            sys.stdin = old_stdin
            sys.stdout = old_stdout
            sys.stderr = old_stderr

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._tty_out.flush()
        except Exception:
            pass
        try:
            self._tty_in.close()
        except Exception:
            pass
        try:
            self._tty_out.close()
        except Exception:
            pass


_DEBUGGER: DagsterTtyPdb | None = None


def _get_debugger() -> DagsterTtyPdb:
    global _DEBUGGER
    if _DEBUGGER is None or _DEBUGGER._closed:
        _DEBUGGER = DagsterTtyPdb()
    return _DEBUGGER


def set_trace() -> None:
    _get_debugger().set_trace(sys._getframe().f_back)


@atexit.register
def _cleanup_debugger() -> None:
    global _DEBUGGER
    if _DEBUGGER is not None:
        _DEBUGGER.close()
        _DEBUGGER = None
