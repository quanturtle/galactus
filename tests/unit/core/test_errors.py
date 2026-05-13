"""The exception hierarchy: per-source errors nest under their stage error, infra
adapter errors nest under InfraError, and everything is a PipelineError."""

from galactus.core.errors import (
    DatabaseError,
    ExtractError,
    HttpError,
    InfraError,
    ParserError,
    PipelineError,
    ScraperError,
    TransformError,
)


def test_per_source_errors_subclass_their_stage_error() -> None:
    assert issubclass(ScraperError, ExtractError)
    assert issubclass(ParserError, TransformError)


def test_infra_errors_subclass_infra_error() -> None:
    assert issubclass(HttpError, InfraError)
    assert issubclass(DatabaseError, InfraError)


def test_everything_is_a_pipeline_error() -> None:
    for exc in (
        ExtractError,
        TransformError,
        ScraperError,
        ParserError,
        InfraError,
        HttpError,
        DatabaseError,
    ):
        assert issubclass(exc, PipelineError)
