from sql.a_bronze.snapshot import Snapshot


class FailedSnapshot(Snapshot):
    """bronze.failed_snapshots — dead-letter table for failed requests: a non-2xx
    response, or a transport error stored as a status_code=0 sentinel."""

    __tablename__ = "failed_snapshots"
