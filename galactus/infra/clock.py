from datetime import UTC, datetime


class SystemClock:
    """Default Clock implementation backed by datetime.now(UTC)."""

    def now(self) -> datetime:
        return datetime.now(UTC)
