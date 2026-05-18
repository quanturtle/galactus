class PipelineError(Exception):
    """Root of all galactus pipeline errors."""


# per stage — raised by stages, often wrapping a per-source error with context
class ExtractError(PipelineError):
    """ExtractStage could not complete."""


class TransformError(PipelineError):
    """TransformStage could not complete."""


class LoadError(PipelineError):
    """LoadStage could not complete."""


# per source — raised by plugin code (scrapers, parsers); each is a kind of its stage's error
class ScraperError(ExtractError):
    """A single source failed to fetch."""


class ParserError(TransformError):
    """A single source failed to parse."""


# raised at startup by load_config and import_plugins
class ConfigError(PipelineError):
    """Bad or missing configuration: file not found, YAML parse error, validation failure, unknown plugin."""


# raised by infra adapters (http, db); plugin code catches these and re-raises as ScraperError / ParserError
class InfraError(PipelineError):
    """An I/O adapter operation failed."""


class HttpError(InfraError):
    """An HTTP request failed: bad status (>=500) or a transport error."""


class DatabaseError(InfraError):
    """A database operation failed."""
