class PipelineError(Exception):
    """Root of all galactus pipeline errors."""


# per source — raised by plugin code (scrapers, parsers)
class ScraperError(PipelineError):
    """A single source failed to fetch."""


class ParserError(PipelineError):
    """A single source failed to parse."""


# per stage — raised by stages, often wrapping a per-source error with context
class ExtractError(PipelineError):
    """ExtractStage could not complete."""


class TransformError(PipelineError):
    """TransformStage could not complete."""


class LoadError(PipelineError):
    """LoadStage could not complete."""
