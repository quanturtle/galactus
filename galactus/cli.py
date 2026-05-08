import argparse
import asyncio
import importlib
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

from galactus.config import PipelineConfig, load_config
from galactus.core.errors import ConfigError, PipelineError
from galactus.core.pipeline import Pipeline
from galactus.extract.stage import ExtractStage
from galactus.infra.logging import setup_logging
from galactus.load.stage import LoadStage
from galactus.transform.stage import TransformStage

logger = logging.getLogger(__name__)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="galactus")
    parser.add_argument("--config", required=True, help="path to per-source YAML config")
    parser.add_argument("--stage", default=None, help="run only one stage by name")
    return parser.parse_args(argv)


def validate_plugins(config: PipelineConfig) -> None:
    """Fail fast if the configured scraper/parser modules can't be imported or don't expose the strategy class."""
    try:
        if config.extract is not None:
            mod = importlib.import_module(f"galactus.extract.scrapers.{config.extract.scraper}")
            if not hasattr(mod, "Scraper"):
                raise ConfigError(
                    f"plugin {config.extract.scraper!r} for {config.name!r} does not export a Scraper class"
                )
        if config.transform is not None:
            mod = importlib.import_module(f"galactus.transform.parsers.{config.transform.parser}")
            if not hasattr(mod, "Parser"):
                raise ConfigError(
                    f"plugin {config.transform.parser!r} for {config.name!r} does not export a Parser class"
                )
    except ImportError as exc:
        raise ConfigError(f"plugin load failed for {config.name!r}: {exc}") from exc
    return


def build_pipeline(config: PipelineConfig) -> Pipeline:
    return Pipeline(
        stages=[
            ExtractStage(config=config),
            TransformStage(config=config),
            LoadStage(config=config),
        ]
    )


async def run(config: PipelineConfig, stage: str | None) -> None:
    pipeline = build_pipeline(config)
    await pipeline.run(stage_name=stage)
    return


def main() -> int:
    load_dotenv()
    args = parse_args(sys.argv[1:])

    # load and validate config
    try:
        config = load_config(Path(args.config))
    except ConfigError as exc:
        logging.basicConfig()
        logging.getLogger(__name__).error("%s", exc)
        return 2

    setup_logging(config.log_level)

    # wire plugins and run pipeline
    try:
        validate_plugins(config)
        asyncio.run(run(config, stage=args.stage))
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
