from dataclasses import dataclass

from galactus.core.interfaces import Clock


@dataclass(frozen=True, slots=True)
class Deps:
    """Long-lived runtime dependencies shared across all stages.

    Per-source resources (HttpClient, Database) are opened by stages directly
    in their per-source loop and not held here.
    """

    clock: Clock
