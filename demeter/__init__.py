"""The database schema for agronomic modeling and supporting data science activities."""

from demeter._version import __version__

from . import data, db, task, work

__all__ = [
    "__version__",
    "data",
    "task",
    "work",
    "db",
]
