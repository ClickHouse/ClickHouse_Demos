"""Makes `import run` resolve from tests/.

pytest inserts the directory holding a conftest.py at the front of sys.path, so this
file existing beside run.py is what lets tests/test_run.py import it however pytest is
invoked -- `python -m pytest tests` happens to work because `-m` also puts the working
directory on sys.path, but a bare `pytest tests` would not.

Mirrors app/conftest.py for the same reason.
"""
