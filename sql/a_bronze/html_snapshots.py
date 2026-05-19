from sql.a_bronze.snapshot import Snapshot


class HtmlSnapshot(Snapshot):
    """bronze.html_snapshots — captured HTML page responses."""

    __tablename__ = "html_snapshots"
