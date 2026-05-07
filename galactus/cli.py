import argparse
import asyncio
import importlib
import logging
import sys
from pathlib import Path

from galactus.config import PipelineConfig, load_config
from galactus.core.errors import ConfigError, PipelineError
from galactus.core.pipeline import Pipeline
from galactus.extract.registry import SCRAPERS
from galactus.extract.stage import ExtractStage
from galactus.infra.logging import setup_logging
from galactus.load.stage import LoadStage
from galactus.transform.registry import PARSERS
from galactus.transform.stage import TransformStage

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = Path("galactus.yaml")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="galactus")
    parser.add_argument(
        "--config", default=None, help="path to YAML config (default: ./galactus.yaml)"
    )
    parser.add_argument(
        "--source",
        action="append",
        default=[],
        help="run only the named source (repeat to run several; default: all)",
    )
    parser.add_argument("--stage", default=None, help="run only one stage by name")
    return parser.parse_args(argv)


def import_plugins(config: PipelineConfig) -> None:
    """Import each source's plugin modules so their @register decorators fire.

    Also validates that the configured scraper/parser names resolve in the
    registries — raises ConfigError if a module or name is unknown.
    """
    for src in config.sources:
        try:
            if src.extract is not None:
                importlib.import_module(src.extract.module)
                SCRAPERS.get(src.extract.scraper)
            if src.transform is not None:
                importlib.import_module(src.transform.module)
                PARSERS.get(src.transform.parser)
        except (ImportError, KeyError) as exc:
            raise ConfigError(f"plugin wiring failed for {src.name!r}: {exc}") from exc
    return


def build_pipeline(config: PipelineConfig) -> Pipeline:
    return Pipeline(
        stages=[
            ExtractStage(config=config),
            TransformStage(config=config),
            LoadStage(config=config),
        ]
    )


async def run(config: PipelineConfig, *, sources: list[str], stage: str | None) -> None:
    pipeline = build_pipeline(config)
    await pipeline.run(sources=sources, stage_name=stage)
    return


def main() -> int:
    args = parse_args(sys.argv[1:])

    # load and validate config
    try:
        config = load_config(Path(args.config) if args.config else DEFAULT_CONFIG)
    except ConfigError as exc:
        logging.basicConfig()
        logging.getLogger(__name__).error("%s", exc)
        return 2

    setup_logging(config.log_level)

    # wire plugins and run pipeline
    try:
        import_plugins(config)
        asyncio.run(run(config, sources=args.source, stage=args.stage))
    except ConfigError as exc:
        logger.error("%s", exc)
        return 2
    except PipelineError as exc:
        logger.error("pipeline failed: %s", exc)
        return 1
    except ValueError as exc:
        logger.error("bad argument: %s", exc)
        return 2
    return 0
