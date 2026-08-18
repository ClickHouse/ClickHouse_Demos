"""Makes `import writer` resolve from tests/.

pytest inserts the directory holding a conftest.py at the front of sys.path, so
this file existing beside writer.py is what lets tests/test_writer.py import it
however pytest is invoked -- `python -m pytest tests` happens to work because
`-m` also puts the working directory on sys.path, but a bare `pytest tests`
would not.
"""
