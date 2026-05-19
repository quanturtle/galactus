from sql.a_bronze.snapshot import Snapshot


class ApiSnapshot(Snapshot):
    """bronze.api_snapshots — captured JSON API responses."""

    __tablename__ = "api_snapshots"
