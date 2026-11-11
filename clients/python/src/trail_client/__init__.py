"""Send events, metrics, errors and logs to Trail. Standard library only.

Application code calls the typed functions `trail gen` writes; those call `Client.record_event`
and `Client.record_metric`. See `spec/client.md` in the Trail repository.
"""

from ._client import Client, Context, Problem, Stats, TrailError
from ._uuid import uuid7

__all__ = ["Client", "Context", "Problem", "Stats", "TrailError", "uuid7"]
__version__ = "0.1.0"
